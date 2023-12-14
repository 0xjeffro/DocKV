package DocKV

import "encoding/json"

func checkError(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func deserialize(data string, model interface{}) interface{} {
	err := json.Unmarshal([]byte(data), &model)
	checkError(err)
	return model
}

func serialize(model interface{}) string {
	data, err := json.Marshal(model)
	checkError(err)
	return string(data)
}
