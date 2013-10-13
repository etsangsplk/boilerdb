package json

import (
	encoder "encoding/json"
	"fmt"
	"strconv"
)

type JsonQuery struct {
	Blob interface{}
}

// Create a new JsonQuery obj from a json-decoded interface{}
func NewQuery(data interface{}) *JsonQuery {
	j := new(JsonQuery)
	j.Blob = data.(map[string]interface{})
	return j
}

// Extract a string from some json
func (j *JsonQuery) String(s ...string) (string, error) {
	val, err := rquery(j.Blob, s...)
	if err != nil {
		return "", err
	}

	ret, err := encoder.Marshal(val)
	return string(ret), err

	//	switch val.(type) {
	//	case string:
	//		return val.(string), nil
	//	}
	//	return "", fmt.Errorf("Expected string value for String, got \"%v\"\n", val)
}

// Extract an object from some json
func (j *JsonQuery) Object(s ...string) (map[string]interface{}, error) {
	val, err := rquery(j.Blob, s...)
	if err != nil {
		return map[string]interface{}{}, err
	}
	switch val.(type) {
	case map[string]interface{}:
		return val.(map[string]interface{}), nil
	}
	return map[string]interface{}{}, fmt.Errorf("Expected json object for Object, get \"%v\"\n", val)
}

// Recursively query a decoded json blob and set its new value
func (jq *JsonQuery) Set(newVal interface{}, s ...string) error {
	var (
		val     interface{}
		err     error
		current interface{} = jq.Blob
		prevQ   string
	)
	val = jq.Blob

	for _, q := range s {
		current = val
		val, err = query(val, q)
		if err != nil {
			return err
		}
		prevQ = q

	}

	switch current.(type) {
	case nil:
		return fmt.Errorf("Nil value found at %s\n", s[len(s)-1])
	case []interface{}:
		arr := current.([]interface{})
		index, err := strconv.Atoi(prevQ)
		if err != nil {
			return fmt.Errorf("Could not access %s in array", prevQ)
		}
		if index > len(arr) {
			return fmt.Errorf("Index out of range")
		}
		arr[index] = newVal
	case map[string]interface{}:
		dict, _ := current.(map[string]interface{})
		dict[prevQ] = newVal
	default:
		return fmt.Errorf("Invalid type for object")

	}
	return nil
}

// Recursively query a decoded json blob
func rquery(blob interface{}, s ...string) (interface{}, error) {
	var (
		val interface{}
		err error
	)
	val = blob
	for _, q := range s {
		val, err = query(val, q)
		if err != nil {
			return nil, err
		}
	}
	switch val.(type) {
	case nil:
		return nil, fmt.Errorf("Nil value found at %s\n", s[len(s)-1])
	}
	return val, nil
}

// Query a json blob for a single field or index.  If query is a string, then
// the blob is treated as a json object (map[string]interface{}).  If query is
// an integer, the blob is treated as a json array ([]interface{}).  Any kind
// of key or index error will result in a nil return value with an error set.
func query(blob interface{}, query string) (interface{}, error) {
	index, err := strconv.Atoi(query)
	// if it's an integer, then we treat the current interface as an array
	if err == nil {
		switch blob.(type) {
		case []interface{}:
		default:
			return nil, fmt.Errorf("Array index on non-array %v\n", blob)
		}
		if len(blob.([]interface{})) > index {
			return blob.([]interface{})[index], nil
		}
		return nil, fmt.Errorf("Array index %d on array %v out of bounds\n", index, blob)
	}

	// blob is likely an object, but verify first
	switch blob.(type) {
	case map[string]interface{}:
	default:
		return nil, fmt.Errorf("Object lookup \"%s\" on non-object %v\n", query, blob)
	}

	val, ok := blob.(map[string]interface{})[query]
	if !ok {
		return nil, nil
		//return nil, fmt.Errorf("Object %v does not contain field %s\n", blob, query)
	}
	return val, nil
}
