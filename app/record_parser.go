package main

import (
	"encoding/binary"
	"io"
	"log"
)

type Record struct {
	values []interface{}
}

// parseRecord parses SQLite's "Record Format", as mentioned here: https://www.sqlite.org/fileformat.html#record_format
func parseRecord(stream io.Reader, valuesCount int) Record {
	parseVarint(stream) // number of bytes in header

	serialTypes := make([]int, valuesCount)

	for i := 0; i < valuesCount; i++ {
		serialTypes[i] = parseVarint(stream)
	}

	values := make([]interface{}, valuesCount)

	for i, serialType := range serialTypes {
		values[i] = parseRecordValue(stream, serialType)
	}

	return Record{values}
}

func parseRecordValue(stream io.Reader, serialType int) interface{} {
	if serialType >= 13 && serialType%2 == 1 {
		// Text encoding
		bytesCount := (serialType - 13) / 2
		value := make([]byte, bytesCount)
		_, _ = stream.Read(value)
		return value
	} else if serialType == 1 {
		// 8 bit twos-complement integer
		return parseUInt8(stream)
	} else if serialType == 4 {
		return parseUInt32(stream)
	} else if serialType == 0 {
		return nil
	} else if serialType >= 12 && serialType%2 == 0 {
		bytesCount := (serialType - 12) / 2
		value := make([]byte, bytesCount)
		err := binary.Read(stream, binary.BigEndian, &value)
		if err != nil {
			log.Fatal("Don't know what this is")
		}
		return string(value)
	}
	// } else if serialType == 10 {
	// 	return nil
	// } else if serialType == 9 {
	// 	return 1

	log.Fatalf("Unknown serial type %v", serialType)
	return nil
}
