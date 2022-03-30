package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

type SQLiteSchemaRow struct {
	_type    string // _type since type is a reserved keyword
	name     string
	tblName  string
	rootPage int
	sql      string
}

type SQLiteSchemaInfo []SQLiteSchemaRow

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	_, _ = databaseFile.Seek(16, io.SeekStart)

	pageSize := parseUInt16(databaseFile)

	_, _ = databaseFile.Seek(100, io.SeekStart) // Skip the database header

	pageHeader := parsePageHeader(databaseFile)

	cellPointers := make([]uint16, pageHeader.NumberOfCells)

	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		cellPointers[i] = parseUInt16(databaseFile)
	}

	var sqliteSchemaRows SQLiteSchemaInfo

	for _, cellPointer := range cellPointers {
		_, _ = databaseFile.Seek(int64(cellPointer), io.SeekStart)
		parseVarint(databaseFile) // number of bytes in payload
		parseVarint(databaseFile) // rowid
		record := parseRecord(databaseFile, 5)

		itemType := string(record.values[0].([]byte))

		if strings.ToUpper(itemType) == "TABLE" {
			sqliteSchemaRows = append(sqliteSchemaRows, SQLiteSchemaRow{
				_type:    string(record.values[0].([]byte)),
				name:     string(record.values[1].([]byte)),
				tblName:  string(record.values[2].([]byte)),
				rootPage: int(record.values[3].(uint8)),
				sql:      string(record.values[4].([]byte)),
			})
		}
	}

	switch command {
	case ".dbinfo":

		fmt.Printf("number of tables: %v", len(sqliteSchemaRows))

	case ".tables":

		var tablesStr string

		for i, tableRow := range sqliteSchemaRows {
			tablesStr += tableRow.tblName
			if i != len(sqliteSchemaRows)-1 {
				tablesStr += " "
			}
		}

		fmt.Println(tablesStr)

	default:
		// If SQL is directly provided for counting the number of rows in a table
		if containsAllStrings(command, []string{"count", "select", "from"}) {
			commandStrSplit := strings.Split(command, " ")
			if len(commandStrSplit) != 4 {
				fmt.Println("Invalid COUNT statement provided as input")
				os.Exit(1)
			}
			tableName := commandStrSplit[3]
			tableInfo := sqliteSchemaRows.findTable(tableName)

			if tableInfo == nil {
				fmt.Printf("Failed to find table \"%s\" in the database\n", tableName)
				os.Exit(1)
			}

			pageOffset := getPageOffset(int(pageSize), tableInfo.rootPage)

			// Move to the page of the table in query
			_, _ = databaseFile.Seek(pageOffset, io.SeekStart)

			newPageHeader := parsePageHeader(databaseFile)

			fmt.Println(newPageHeader.NumberOfCells) // for the number of rows in the table
		} else if containsAllStrings(command, []string{"select", "from"}) {
			commandStrSplit := strings.Split(command, " ")
			sqlContainsWhere := strings.Contains(command, "where")
			var tableName string
			if !sqlContainsWhere {
				tableName = commandStrSplit[len(commandStrSplit)-1]
			} else {
				// TODO: try to keep this consistent somehow? make either uppercase or lowercase the default
				whereSp := strings.Split(command, "where")
				whereSp2 := strings.Split(whereSp[0], " ")
				tableName = whereSp2[len(whereSp2)-2]
			}
			tableInfo := sqliteSchemaRows.findTable(tableName)

			tableColumns := parseCreateTableSQLForColumnNames(tableInfo.sql)
			selectedColumns := parseSelectFromTableSQLForColumnNames(command)

			intersectionExists := checkIntersectionOfColumns(tableColumns, selectedColumns)

			if !intersectionExists {
				fmt.Println("No Rows found with given column(s)")
				return
			}

			columnIndexes := getColumnIndexes(tableColumns, selectedColumns)

			pageOffset := getPageOffset(int(pageSize), tableInfo.rootPage)

			// Move to the page of the table in query
			_, _ = databaseFile.Seek(pageOffset, io.SeekStart)

			newPageHeader := parsePageHeader(databaseFile)

			newCellPointers := make([]uint16, newPageHeader.NumberOfCells)

			for i := 0; i < int(newPageHeader.NumberOfCells); i++ {
				newCellPointers[i] = parseUInt16(databaseFile)
			}

			rowValues := make([][]string, 0)

			for _, cellPointer := range newCellPointers {
				_, _ = databaseFile.Seek(pageOffset+int64(cellPointer), io.SeekStart)
				parseVarint(databaseFile) // number of bytes in payload
				parseVarint(databaseFile) // rowid
				newrecord := parseRecord(databaseFile, len(tableColumns))
				row := make([]string, 0)
				for _, val := range newrecord.values {
					if val == nil {
						row = append(row, "")
						continue
					}
					// FIXME/TODO/NOTE: this is done assuming that we only have columns with type text
					row = append(row, string(val.([]byte)))
				}
				rowValues = append(rowValues, row)
			}

			rowSelectResults := make([][]string, 0)

			for _, r := range rowValues {
				row := make([]string, 0)
				for _, idx := range columnIndexes {
					row = append(row, r[idx])
				}
				rowSelectResults = append(rowSelectResults, row)
			}

			if sqlContainsWhere { // if there's a WHERE clause...
				whereSplit := strings.Split(command, "where")
				conditionSplit := strings.Split(strings.TrimSpace(whereSplit[1]), " = ")
				conditionCol := conditionSplit[0]
				conditionVal := parseColumnValueString(conditionSplit[1])

				rs := make([][]string, 0)
				for _, r := range rowValues {
					for _, idx := range columnIndexes {
						colName := tableColumns[idx]
						if colName == conditionCol && conditionVal == r[idx] {
							rs = append(rs, r)
						}
					}
				}

				newRowSelectResults := make([][]string, 0)
				for _, r := range rs {
					row := make([]string, 0)
					for _, idx := range columnIndexes {
						row = append(row, r[idx])
					}
					newRowSelectResults = append(newRowSelectResults, row)
				}

				rowSelectResults = newRowSelectResults
			}

			// BUG: SELECT name FROM apples where color = 'Yellow' - doesn't give any rows in the result

			if len(rowSelectResults) == 0 {
				fmt.Println("No Rows found with given column(s)")
				return
			}

			for _, r := range rowSelectResults {
				fmt.Println(strings.Join(r, "|"))
			}
		} else {
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	}
}

func (s SQLiteSchemaInfo) findTable(tableName string) *SQLiteSchemaRow {
	for _, tableInfo := range s {
		if tableInfo.tblName == tableName {
			return &tableInfo
		}
	}

	return nil
}

func containsAllStrings(s string, strs []string) bool {
	for _, st := range strs {
		if !strings.Contains(strings.ToUpper(s), strings.ToUpper(st)) {
			return false
		}
	}

	return true
}

func parseColumnValueString(str string) string {
	re := regexp.MustCompile(`(?msi)'(\w+)'`)
	res := re.FindAllStringSubmatch(str, -1)

	return res[0][1]
}

func parseCreateTableSQLForColumnNames(sql string) []string {
	tableColumns := make([]string, 0)
	var re = regexp.MustCompile(`(?mi)create table\s+\w+\s+\(([\w\s"-_,]+)\)`)

	res := re.FindAllStringSubmatch(sql, -1)

	if len(res) <= 0 {
		log.Fatalf("Failed to parse column names from SQL: %s \n", sql)
	}

	columnStr := strings.TrimSpace(res[0][1])
	columnSplit := strings.Split(columnStr, ",")

	for _, col := range columnSplit {
		colSpl := strings.Split(col, " ")
		tableColumns = append(tableColumns, strings.ToLower(strings.TrimSpace(colSpl[0])))
	}

	return tableColumns
}

func parseSelectFromTableSQLForColumnNames(sql string) []string {
	tableColumns := make([]string, 0)
	var re = regexp.MustCompile(`(?mi)select\s+([\w\s,-]+)\s+from.+`)

	res := re.FindAllStringSubmatch(sql, -1)

	if len(res) <= 0 {
		log.Fatalf("Failed to parse column names from SQL: %s \n", sql)
	}

	columnStr := res[0][1]
	columnSplit := strings.Split(columnStr, ",")

	for _, col := range columnSplit {
		tableColumns = append(tableColumns, strings.ToLower(strings.TrimSpace(col)))
	}

	return tableColumns
}

func getPageOffset(pageSize, rootPage int) int64 {
	return (int64(pageSize) * int64(rootPage)) - int64(pageSize)
}

func getColumnIndexes(tableColumns, selectedColumns []string) []int {
	indexes := make([]int, 0)
	for _, selCol := range selectedColumns {
		for i, col := range tableColumns {
			if col == selCol {
				indexes = append(indexes, i)
				break
			}
		}
	}
	return indexes
}

func checkIntersectionOfColumns(tableColumns, selectedColumns []string) bool {
	intersectionExists := true

	for _, c := range selectedColumns {
		assumptionContains := false
		for _, cl := range tableColumns {
			if c == cl {
				assumptionContains = true
				break
			}
		}
		if !assumptionContains {
			intersectionExists = false
			break
		}
	}

	return intersectionExists
}
