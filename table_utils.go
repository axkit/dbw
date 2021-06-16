package dbw

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var PrimaryKeyFieldName = "ID"

// HasAutoincrementFieldID returns true if struct has field ID with
// one of type: int8, int16, int32, int64 and has no tag "noseq".
func HasAutoincrementFieldID(model interface{}) bool {

	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	for i := 0; i < tof.NumField(); i++ {

		tf := tof.Field(i)
		sf := s.Field(i)

		if tf.Anonymous {
			res := false
			if tf.Type.Kind() != reflect.Ptr {
				res = HasAutoincrementFieldID(sf.Addr().Interface())
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					res = HasAutoincrementFieldID(mock.Interface())
				} else {
					res = HasAutoincrementFieldID(sf.Interface())
				}
			}

			if res == true {
				return true
			}
		}

		tag := tf.Tag.Get(FieldTagLabel)
		if tf.Name == PrimaryKeyFieldName {
			if len(tag) > 0 && anyTagContains(tag, TagNoSeq) == true {
				return false
			}
			return strings.HasPrefix(tf.Type.Kind().String(), "int")
		}
	}
	return false
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// ToSnakeCase converts string like RobertEgorov to robert_egorov.
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// tagContains returns true if in fieldTag `dbw:"noseq,x,maxlen=4"` (without prefix dbw)
// contains at least one label "noseq".
func anyTagContains(fieldTag, tagRules string) bool {

	if len(tagRules) == 0 || len(fieldTag) == 0 {
		return false
	}

	if c := fieldTag[len(fieldTag)-1]; c < '0' || c > 'z' {
		panic("Invalid struct field tag: " + fieldTag)
	}

	from := 0
	var t string
	for {
		to := strings.Index(tagRules[from:], ",")
		if to != -1 {
			t = tagRules[from : from+to]
			from = from + to + 1
		} else {
			t = tagRules[from:]
		}

		for {
			idx := strings.Index(fieldTag, t)
			if idx == -1 {
				break
			}

			if len(fieldTag) == idx+len(t) {
				// found in the end of the string
				return true
			}

			if c := fieldTag[idx+len(t)]; c == ',' || c == '=' {
				return true
			}
		}

		if to == -1 {
			break
		}
	}
	return false
}

// fieldAddrs returns slice of pointers to struct's fields excluding fields
// having tag value specified in excludeTag
func FieldAddrs(model interface{}, excludeTag string) []interface{} {

	//println("excludeTag", excludeTag)
	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	res := make([]interface{}, 0, s.NumField())

	for i := 0; i < s.NumField(); i++ {
		sf := s.Field(i)
		tf := tof.Field(i)

		// ignore private fields.
		if sf.CanSet() == false {
			continue
		}

		tag := tf.Tag.Get(FieldTagLabel)
		//println("fieldName=", tf.Name, "tag", tag, "excludedTag", excludeTag)
		if tag == "-" {
			continue
		}

		if tf.Name == "ID" && strings.HasPrefix(tf.Type.String(), "int") {
			if len(tag) == 0 || (len(tag) > 0 && anyTagContains(tag, TagNoSeq) == false) {
				continue
			}
		} else {
			if anyTagContains(tag, excludeTag) == true {
				println("BAM")
				continue
			}
		}

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				res = append(res, FieldAddrs(sf.Addr().Interface(), excludeTag)...)
			} else {
				if sf.IsNil() {
					panic("Non initialised embedded structure: " + sf.String())
				}
				res = append(res, FieldAddrs(sf.Interface(), excludeTag)...)
			}
			continue
		}
		res = append(res, sf.Addr().Interface())
	}
	return res
}

func FieldAddrsUpdate(model interface{}, excludeTag string) (res []interface{}, updatedAt *NullTime, rowVersion *int) {

	var id interface{}
	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	res = make([]interface{}, 0, s.NumField())

	for i := 0; i < s.NumField(); i++ {
		sf := s.Field(i)
		tf := tof.Field(i)

		// ignore private fields.
		if sf.CanSet() == false {
			continue
		}

		tag := tf.Tag.Get(FieldTagLabel)
		if tag == "-" {
			continue
		}

		if tf.Name == "CreatedAt" {
			continue
		}

		if tf.Name == "ID" {
			id = sf.Addr().Interface()
			continue
		} else {
			if tf.Name == "UpdatedAt" {
				updatedAt = sf.Addr().Interface().(*NullTime)
				if !updatedAt.Valid() {
					updatedAt.SetNow() // !
				}
			} else if tf.Name == "RowVersion" {
				rowVersion = sf.Addr().Interface().(*int)
				continue
			}

			if anyTagContains(tag, excludeTag) == true {
				continue
			}

		}

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				res = append(res, FieldAddrs(sf.Addr().Interface(), excludeTag)...)
			} else {
				if sf.IsNil() {
					panic("Non initialised embedded structure: " + sf.String())
				}
				res = append(res, FieldAddrs(sf.Interface(), excludeTag)...)
			}
			continue
		}
		res = append(res, sf.Addr().Interface())
	}
	if id != nil {
		res = append(res, id)
	}
	return
}

// FieldNames returns names of struct fields separated by comma.
// Includes anonymous structs as well.
func FieldNames(model interface{}, tagRules string) string {

	var res, sep string

	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	for i := 0; i < tof.NumField(); i++ {

		tf := tof.Field(i)
		sf := s.Field(i)

		// ignore private fields.
		if sf.CanSet() == false {
			continue
		}

		tag := tf.Tag.Get(FieldTagLabel)
		if tag == "-" {
			continue
		}

		if anyTagContains(tag, tagRules) == true {
			continue
		}

		if tf.Anonymous {
			var ss string
			if tf.Type.Kind() != reflect.Ptr {
				ss = FieldNames(sf.Addr().Interface(), tagRules)
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					ss = FieldNames(mock.Interface(), tagRules)
				} else {
					ss = FieldNames(sf.Interface(), tagRules)
				}
			}
			if len(ss) > 0 {
				res += sep + ss
			}
		} else {
			res += sep + ToSnakeCase(tf.Name)
		}
		sep = ", "
	}
	return res
}

func formatQueryParams(addrs []interface{}) string {
	var s, sep string
	for i := range addrs {
		s = s + sep + "$" + strconv.Itoa(i+1) + "=" + fmt.Sprintf("%s", addrs[i])
		sep = "; "
	}
	return s
}
