package DocKV

import (
	"context"
	"errors"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

type DocKV struct {
	sheet *spreadsheet.Sheet
	model interface{}
	cache *Cache
}

func NewDocKV(sheetID string, model interface{}, jwtConfig []byte, expTime int) *DocKV {
	conf, err := google.JWTConfigFromJSON(jwtConfig, spreadsheet.Scope)
	checkError(err)
	client := conf.Client(context.TODO())
	service := spreadsheet.NewServiceWithClient(client)
	spreadSheet, err := service.FetchSpreadsheet(sheetID)
	checkError(err)
	sheet, err := spreadSheet.SheetByIndex(0)
	// Delete all rows
	sheet.Update(0, 8, "DocKV initialized! (This cell could be deleted)")
	return &DocKV{
		sheet: sheet,
		model: model,
		cache: NewCache(expTime),
	}
}

func (d *DocKV) Get(key string) interface{} {
	if key == "" {
		return errors.New("key can not be empty")
	}
	if d.cache.IsExist(key) && !d.cache.IsExpire(key) { // cache hit
		if d.cache.IsNil(key) {
			// key not exist in sheet
			return nil
		} else {
			return deserialize(d.cache.data[key], d.model)
		}
	} else {
		for idx, row := range d.sheet.Rows {
			if row[0].Value == "" {
				break
			}
			d.cache.Set(key, row[1].Value, idx)
			if row[0].Value == key {
				return deserialize(row[1].Value, d.model)
			}
		}
		// key not exist in sheet
		d.cache.SetNotExistInSheet(key)
		return nil
	}
}

func (d *DocKV) Set(key string, value interface{}) error {
	if key == "" {
		return errors.New("key can not be empty")
	}
	if d.cache.IsExist(key) && !d.cache.IsExpire(key) && !d.cache.IsNil(key) {
		// cache hit
		if d.sheet.Rows[d.cache.rowIndex[key]][0].Value == key {
			d.sheet.Update(d.cache.rowIndex[key], 1, serialize(value))
			err := d.sheet.Synchronize()
			if err != nil {
				return err
			} else {
				d.cache.Set(key, serialize(value), d.cache.rowIndex[key])
				return nil
			}
		} else {
			// cache is not consistent with sheet
			d.cache.Clear()
		}
	}
	keyPos := -1
	emptyRow := len(d.sheet.Rows)
	for idx, row := range d.sheet.Rows {
		if row[0].Value == "" {
			emptyRow = idx
			break
		}
		d.cache.Set(key, row[1].Value, idx)
		if row[0].Value == key {
			keyPos = idx
			break
		}
	}
	if keyPos == -1 {
		// key not exist in sheet
		d.sheet.Update(emptyRow, 0, key)
		d.sheet.Update(emptyRow, 1, serialize(value))
		err := d.sheet.Synchronize()
		if err != nil {
			return err
		} else {
			d.cache.Set(key, serialize(value), emptyRow)
			return nil
		}
	} else {
		// key exist in sheet
		d.sheet.Update(keyPos, 1, serialize(value))
		err := d.sheet.Synchronize()
		if err != nil {
			return err
		} else {
			d.cache.Set(key, serialize(value), keyPos)
			return nil
		}
	}
}

func (d *DocKV) Delete(key string) error {
	if key == "" {
		return errors.New("key can not be empty")
	}
	if d.cache.IsExist(key) && !d.cache.IsExpire(key) && !d.cache.IsNil(key) {
		// cache hit
		if d.sheet.Rows[d.cache.rowIndex[key]][0].Value == key {
			err := d.sheet.DeleteRows(d.cache.rowIndex[key], d.cache.rowIndex[key]+1)
			if err != nil {
				return err
			}
			err = d.sheet.Synchronize()
			if err != nil {
				return err
			} else {
				d.cache.Delete(key)
			}
		} else {
			d.cache.Clear()
		}
	}

	keyPos := -1
	for idx, row := range d.sheet.Rows {
		if row[0].Value == "" {
			break
		}
		d.cache.Set(key, row[1].Value, idx)
		if row[0].Value == key {
			keyPos = idx
			break
		}
	}
	if keyPos == -1 {
		// key not exist in sheet
		return nil
	} else {
		// key exist in sheet
		err := d.sheet.DeleteRows(keyPos, keyPos+1)
		if err != nil {
			return err
		}
		err = d.sheet.Synchronize()
		if err != nil {
			return err
		} else {
			d.cache.Delete(key)
			return nil
		}
	}
}
