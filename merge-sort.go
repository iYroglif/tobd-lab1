package main

import "sync"

func MergeSort(arr *[]string, left int, right int) {
	if right-left <= 1 {
		return
	}

	mid := (left + right) / 2

	if mid-left > 1 {
		MergeSort(arr, left, mid)
	}

	if right-mid > 1 {
		MergeSort(arr, mid, right)
	}

	merge(arr, left, mid, right)
}

func MultiThreadedMergeSort(arr *[]string, left int, right int) {
	if right-left <= 1 {
		return
	}

	mid := (left + right) / 2
	wg := &sync.WaitGroup{}

	if mid-left > 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			MultiThreadedMergeSort(arr, left, mid)
		}()
	}

	if right-mid > 1 {
		MultiThreadedMergeSort(arr, mid, right)
	}

	wg.Wait()

	merge(arr, left, mid, right)
}

func merge(arr *[]string, left, mid, right int) {
	leftHalf := make([]string, mid-left)
	leftIndex := 0
	rightIndex := mid
	arrIndex := left

	copy(leftHalf, (*arr)[left:mid])

	for leftIndex < len(leftHalf) && rightIndex < right {
		if leftHalf[leftIndex] <= (*arr)[rightIndex] {
			// if len(leftHalf[leftIndex]) <= len((*arr)[rightIndex]) {
			(*arr)[arrIndex] = leftHalf[leftIndex]
			leftIndex++
		} else {
			(*arr)[arrIndex] = (*arr)[rightIndex]
			rightIndex++
		}
		arrIndex++
	}

	for leftIndex < len(leftHalf) {
		(*arr)[arrIndex] = leftHalf[leftIndex]
		leftIndex++
		arrIndex++
	}

	leftHalf = nil
}
