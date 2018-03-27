package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strings"
	"strconv"
	"unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	var key_values []mapreduce.KeyValue

	//做完切割，顺便统计一下单词对应出现的次数（囧 好顺便）
	//var str_cnt = make(map[string]int)
	//for _, v := range strings.Split(contents, " ") {
	//	if _, ok := str_cnt[v]; ok {
	//		str_cnt[v] += 1
	//	}else {
	//		str_cnt[v] = 1
	//	}
	//}
	//for str, cnt := range str_cnt{
	//	var key_value mapreduce.KeyValue
	//	key_value.Key = str
	//	key_value.Value = string(cnt)//不能这样用 囧
	//	key_values = append(key_values, key_value)
	//}

	//println(filename, contents)
	//for _, v := range strings.Split(contents, " ") {//不能这样用 囧 Split 只是简单的以空格分隔，还有其他的标点符号问题
	//	var key_value mapreduce.KeyValue
	//	key_value.Key = v
	//	key_value.Value = string(1)
	//	key_values = append(key_values, key_value)
	//}
	//
	//return key_values

	//这里单纯只做切割
	keys := strings.FieldsFunc(contents, func(ch rune) bool {
                return !unicode.IsLetter(ch)
        })

	for _, w := range keys {
		key_values = append(key_values, mapreduce.KeyValue{w, strconv.Itoa(1)})
	}

	return key_values
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// Your code here (Part II).
	var sum int
	for _,cnt := range values {
		int_cnt,err := strconv.Atoi(cnt)
		if err == nil {
			sum += int_cnt
		}

	}
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
