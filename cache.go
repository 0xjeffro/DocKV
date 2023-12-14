package DocKV

import "time"

type Cache struct {
	data       map[string]string
	updateTime map[string]int64
	rowIndex   map[string]int
	expTime    int
}

func NewCache(expTime int) *Cache {
	return &Cache{
		data:       make(map[string]string),
		updateTime: make(map[string]int64),
		rowIndex:   make(map[string]int),
		expTime:    expTime,
	}
}

// IsExist whether key exist in cache
func (c *Cache) IsExist(key string) bool {
	_, ok := c.data[key]
	return ok
}

// IsExpire whether key is expire
func (c *Cache) IsExpire(key string) bool {
	if !c.IsExist(key) {
		panic("key not exist")
	}
	return time.Now().Unix()-c.updateTime[key] > int64(c.expTime)
}

// IsNil whether key is exist but not in sheet
func (c *Cache) IsNil(key string) bool {
	return c.rowIndex[key] == -1
}

// Set cache
func (c *Cache) Set(key, value string, rowIndex int) {
	c.data[key] = value
	c.updateTime[key] = time.Now().Unix()
	c.rowIndex[key] = rowIndex
}

func (c *Cache) SetNotExistInSheet(key string) {
	c.data[key] = ""
	c.updateTime[key] = time.Now().Unix()
	c.rowIndex[key] = -1
}

// Delete key from cache
func (c *Cache) Delete(key string) {
	c.data[key] = ""
	c.updateTime[key] = time.Now().Unix()
	c.rowIndex[key] = -1
	for k, v := range c.rowIndex {
		if v > c.rowIndex[key] {
			c.rowIndex[k]--
		}
	}
}

// Clear cache
func (c *Cache) Clear() {
	c.data = make(map[string]string)
	c.updateTime = make(map[string]int64)
	c.rowIndex = make(map[string]int)
}
