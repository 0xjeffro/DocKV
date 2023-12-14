package DocKV

import (
	"os"
	"testing"
)

type Model struct {
	Name   string `json:"name"`
	Age    int    `json:"age"`
	Gender bool   `json:"gender"`
}

func connect() *DocKV {
	clientSecret := os.Getenv("CLIENT_SECRET")
	sheetID := os.Getenv("SHEET_ID")
	return NewDocKV(sheetID, Model{}, []byte(clientSecret), 30)
}

func TestSet(t *testing.T) {
	DocKV := connect()
	err := DocKV.Set("John", Model{
		Name: "John",
		Age:  18,
	})
	if err != nil {
		t.Error(err)
	}
	err = DocKV.Set("Amy", Model{
		Name: "Amy",
		Age:  18,
	})
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 1000; i++ {
		name := "John" + string(rune(i))
		err = DocKV.Set(name, Model{
			Name: name,
			Age:  i,
		})
		if err != nil {
			t.Error(err)
		}
	}
}
