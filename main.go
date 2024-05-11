package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

const (
	fileName        = "text.txt"
	sortedFileName  = "sorted.txt"
	maxStringLength = 32
	numRows         = 1024 * 1024 * 1024 / (maxStringLength/2 + 1)
)

func generateRandomChar() string {
	randomNumber := rand.Intn(26)
	charCode := byte('A' + randomNumber)

	return string(charCode)
}

func generateRandomString(length int) string {
	var result string
	for i := 0; i < length; i++ {
		result += generateRandomChar()
	}
	return result
}

func generateFile(fileName string, numRows int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	for i := 0; i < numRows; i++ {
		randomStringLength := 1 + rand.Intn(maxStringLength)
		randomString := generateRandomString(randomStringLength)

		_, err = file.WriteString(randomString + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func readFileToArray(fileName string) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	rows := make([]string, 0, numRows+1)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rows = append(rows, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func readOrGenerateFile() ([]string, error) {
	if _, err := os.Stat(fileName); err == nil {
		fmt.Println("File already exists")
	} else {
		err := generateFile(fileName, numRows)
		if err != nil {
			fmt.Println("Error generating file:", err)
			return nil, err
		}

		fmt.Println("File generated successfully")
	}

	rows, err := readFileToArray(fileName)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return nil, err
	}

	return rows, nil
}

func writeSortedFile(rows []string) {
	if _, err := os.Stat(sortedFileName); err == nil {
		fmt.Println("Sorted file already exists")
		return
	}

	file, err := os.Create(sortedFileName)
	if err != nil {
		fmt.Println("Error creating sorted file:", err)
		return
	}
	defer file.Close()

	for _, row := range rows {
		_, err = file.WriteString(row + "\n")
		if err != nil {
			fmt.Println("Error writing to sorted file:", err)
			return
		}
	}

	fmt.Println("Sorted file written successfully")
}

func main() {
	fmt.Println("Generating file...")

	rows, err := readOrGenerateFile()
	if err != nil {
		return
	}

	fmt.Println()
	fmt.Println("First 10 rows:")
	for i := 0; i < 10; i++ {
		fmt.Println(rows[i])
	}
	fmt.Println()

	start := time.Now()
	// MergeSort(&rows, 0, len(rows))
	MultiThreadedMergeSort(&rows, 0, len(rows))
	end := time.Now()
	sortTime := end.Sub(start)

	// fmt.Println("Sort time for merge sort by string lenght:", sortTime)
	fmt.Println("Sort time for multi threaded merge sort by letters:", sortTime)

	fmt.Println()
	fmt.Println("First 10 sorted rows:")
	for i := 0; i < 10; i++ {
		fmt.Println(rows[i])
	}
	fmt.Println()

	fmt.Println("Writing sorted file...")
	writeSortedFile(rows)

	fmt.Println("Enter any key to exit...")
	fmt.Scanln()
}
