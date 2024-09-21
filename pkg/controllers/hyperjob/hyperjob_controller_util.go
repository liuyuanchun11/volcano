/*
Copyright 2022 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hyperjob

import (
	"strconv"
)

func GetJobName(hyperJobName string, replicatedJobName string, index int) string {
	return hyperJobName + "-" + replicatedJobName + "-" + strconv.Itoa(index)
}

func Concat[T any](slices ...[]T) []T {
	var result = make([]T, 0)
	for _, slice := range slices {
		result = append(result, slice...)
	}
	return result
}

func CloneMap[K, V comparable](m map[K]V) map[K]V {
	cloneMap := make(map[K]V)
	for k, v := range m {
		cloneMap[k] = v
	}
	return cloneMap
}
