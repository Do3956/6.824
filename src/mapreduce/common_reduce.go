package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

//func check(e error) {
//	if e != nil {
//		panic(e)
//	}
//}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var key_values = make(map[string][]string)

	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		//println(i, filename)
		fp, _ := os.Open(filename)
		enc := json.NewDecoder(fp)
		for {
			var v KeyValue
			err := enc.Decode(&v)
			if err != nil {
				break
			}

			if _, ok := key_values[v.Key]; ok {
				key_values[v.Key] = append(key_values[v.Key], v.Value)
			}else {
				key_values[v.Key] = []string {v.Value}
			}

		}
		fp.Close()
	}

	//println("key_values len:", len(key_values))
	sortToFile(key_values, outFile, reduceF)
}

func sortToFile(
	key_values map [string][]string,
	outFile string, // write the output here
	reduceF func(key string, values []string) string,
)  {

    keys := make([]string, len(key_values))
	//println("keys",keys)

    i := 0
    for k, _ := range key_values {
		//println(i,k)
        keys[i] = k
        i++
    }

	//println("keys",keys)
    sort.Strings(keys)

	fp, _ := os.Create(outFile)
    //check(err)
    defer fp.Close()
	enc := json.NewEncoder(fp)

	//sort.Stable(key_values)
	//for key, values := range key_values {
	//	enc.Encode(KeyValue{key, reduceF(key, values)})
	//}
    for _, key := range keys {
		println(key, key_values[key])
		enc.Encode(KeyValue{key, reduceF(key, key_values[key])})
	}
}