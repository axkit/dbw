package dbw

import (
	"reflect"
	"strconv"
	"strings"
)

func (t *Table) genParam(i int) string {
	s := ""
	switch t.DB().PlaceHolderType() {
	case QuestionMark:
		s = "=?"
	case DollarPlusPosition:
		s = "=$" + strconv.Itoa(i)
	}
	return s
}

func (t *Table) genUpdateSQL(fields string) string {
	var s, f string

	s = "UPDATE " + t.name + " SET "
	sep := ""
	i := 1
	from, to := 0, 0
	for {

		to = strings.Index(fields[from:], ", ")
		if to != -1 {
			f = fields[from : from+to]
			from = from + to + 2
		} else {
			f = fields[from:]
		}

		if f == "id" || f == "created_at" || f == "deleted_at" || f == "row_version" || f == "updated_at" {
			if to == -1 {
				if t.withUpdatedAt {
					f = "updated_at"
				}
			} else {
				continue
			}
		}

		s = s + sep + f

		s += t.genParam(i)
		sep = ", "
		i++
		if to == -1 {
			break
		}
	}
	if t.withRowVersion {
		s += sep + "row_version=row_version+1"
	}

	s += " WHERE id="

	switch t.DB().PlaceHolderType() {
	case QuestionMark:
		s += "?"
	case DollarPlusPosition:
		s += "$" + strconv.Itoa(i)
	}

	if t.withRowVersion {
		s += " RETURNING row_version"
	}

	return s
}

func (t *Table) genInsertSQL() string {
	insfn := t.fieldNames(t.model, TagNoIns, Exclude)
	inscnt := strings.Count(insfn, ",") + 1

	s := "INSERT INTO " + t.name + "(" + insfn + ") VALUES("

	if t.isSequenceUsed {
		s += "NEXTVAL('" + t.name + "_seq'), "
		inscnt--
	}

	sep := ""
	for i := 1; i <= inscnt; i++ {
		s += sep

		switch t.DB().PlaceHolderType() {
		case QuestionMark:
			s = s + "?"
		case DollarPlusPosition:
			s = s + "$" + strconv.Itoa(i)
		}

		sep = ", "
	}
	s += ")"

	if t.isSequenceUsed {
		s += " RETURNING id"
	}

	return s
}

func (t *Table) initColTag(model interface{}) {

	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	// if recursive call
	if t.coltag == nil {
		t.coltag = make(map[string]map[string]string, tof.NumField())
	}

	for i := 0; i < tof.NumField(); i++ {

		tf := tof.Field(i)
		sf := s.Field(i)

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				t.initColTag(sf.Addr().Interface())
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					t.initColTag(mock.Interface())
				} else {
					t.initColTag(sf.Interface())
				}
			}
		}

		// split struct field tag by comma.
		arr := strings.Split(tf.Tag.Get(FieldTagLabel), ",")

		_, ok := t.coltag[tf.Name]
		if !ok {
			t.coltag[tf.Name] = make(map[string]string, len(arr))
		}

		t.coltag[tf.Name]["Kind"] = sf.Kind().String()
		t.coltag[tf.Name]["SnakeName"] = ToSnakeCase(tf.Name)
		t.coltag[tf.Name]["DataType"] = tf.Type.String()

		for j := range arr {
			key := arr[j]
			val := ""

			// if struct field tag has value like `dbw:"maxlen=20"`
			if idx := strings.Index(arr[j], "="); idx > 0 {
				if idx == len(arr[j])-1 {
					// if val is empty, and looks like `dbw:"maxlen=,"`
					key = arr[j][:idx]
				} else {
					key = arr[j][:idx]
					val = arr[j][idx+1:]
				}
			}
			t.coltag[tf.Name][key] = val
		}
	}
}

func (t *Table) anyTagContains(fieldName, cstags string) bool {

	if len(cstags) == 0 {
		return false
		//panic("empty param cstags handled to function Table.anyTagContains()")
	}

	from := 0
	var tag string
	for {
		to := strings.Index(cstags[from:], ",")
		if to != -1 {
			tag = cstags[from : from+to]
			from = from + to + 1
		} else {
			tag = cstags[from:]
		}

		if c, ok := t.coltag[fieldName]; ok {

			if _, ok = c[tag]; ok {
				return true
			}
		}

		if to == -1 {
			break
		}
	}
	return false
}

func (t *Table) fieldNames(model interface{}, tags string, rule TagExclusionRule) string {

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

		switch rule {
		case Exclude:
			if t.anyTagContains(tf.Name, tags) == true {
				continue
			}
		case Include:
			if t.anyTagContains(tf.Name, tags) == false {
				continue
			}
		case All:
			break
		default:
			panic("unknown tag exclusion rule")
		}

		if tf.Anonymous {
			var ss string
			if tf.Type.Kind() != reflect.Ptr {
				ss = t.fieldNames(sf.Addr().Interface(), tags, rule)
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					ss = t.fieldNames(mock.Interface(), tags, rule)
				} else {
					ss = t.fieldNames(sf.Interface(), tags, rule)
				}
			}
			if len(ss) > 0 {
				res += sep + ss
			}
		} else {
			res += sep + t.SnakeIt(tf.Name)
		}
		sep = ", "
	}
	return res
}

func (t *Table) fieldNamesUpdate(model interface{}, tags string, rule TagExclusionRule) string {

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

		//fmt.Println("tf=", tf.Name)

		if tf.Anonymous {
			var ss string
			if tf.Type.Kind() != reflect.Ptr {
				ss = t.fieldNamesUpdate(sf.Addr().Interface(), tags, rule)
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					ss = t.fieldNamesUpdate(mock.Interface(), tags, rule)
				} else {
					ss = t.fieldNamesUpdate(sf.Interface(), tags, rule)
				}
			}
			//fmt.Println("ss=", ss, len(ss))
			if len(ss) == 0 {
				continue
			}
			res += sep + ss

		} else {
			if tf.Name != "UpdatedAt" {
				switch rule {
				case Exclude:
					if t.anyTagContains(tf.Name, tags) == true {
						continue
					}
				case Include:
					if t.anyTagContains(tf.Name, tags) == false {
						continue
					}
				case All:
					break
				default:
					panic("unknown tag exclusion rule")
				}
			}

			res += sep + t.SnakeIt(tf.Name)
			//fmt.Println("res=", res)
		}
		sep = ", "
	}
	return res
}

func (t *Table) SnakeIt(fieldName string) string {

	c, ok := t.coltag[fieldName]
	if ok {
		return c["SnakeName"]
	}

	return fieldName
}

// fieldAddrs returns slice of pointers to struct's fields excluding fields
// having tag value specified in excludeTag
func (t *Table) fieldAddrs(model interface{}, cstags string, rule TagExclusionRule) []interface{} {

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
		if tag == "-" {
			continue
		}

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				res = append(res, t.fieldAddrs(sf.Addr().Interface(), cstags, rule)...)
			} else {
				if sf.IsNil() {
					panic("Non initialised embedded structure: " + sf.String())
				}
				res = append(res, t.fieldAddrs(sf.Interface(), cstags, rule)...)
			}
			continue
		}

		if !(tf.Name == "ID" && intoruint(tf.Type.String())) {
			switch rule {
			case Exclude:
				if t.anyTagContains(tf.Name, cstags) == true {
					continue
				}
			case Include:
				if t.anyTagContains(tf.Name, cstags) == false {
					continue
				}
			case All:
				break
			default:
				panic("unknown tag exclusion rule")
			}
		} else {

			// here if field name == "ID" && type is int# or uint#
			if len(tag) == 0 || (len(tag) > 0 && t.anyTagContains("ID", TagNoSeq) == false) {
				continue
			}
		}

		res = append(res, sf.Addr().Interface())
	}
	return res
}

func intoruint(s string) bool {
	return strings.HasPrefix(s, "int") || strings.HasPrefix(s, "uint")
}
func (t *Table) ColTags() string {

	s := ""
	for k, v := range t.coltag {
		s += k + ": "
		for k1, v1 := range v {
			s += " " + k1 + "-" + v1
		}
	}
	return s
}
func (t *Table) fieldAddrsUpdate(model interface{}, cstags string, rule TagExclusionRule) (res []interface{}, updatedAt *NullTime, rowVersion *int) {
	level := 0
	res, updatedAt, rowVersion, _ = t.fieldAddrsUpdate_(model, cstags, rule, &level)
	return
}

func (t *Table) fieldAddrsUpdate_(model interface{}, cstags string, rule TagExclusionRule, level *int) (res []interface{}, updatedAt *NullTime, rowVersion *int, id interface{}) {

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

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				*level++
				r, ua, rv, id_ := t.fieldAddrsUpdate_(sf.Addr().Interface(), cstags, rule, level)
				*level--
				res = append(res, r...)
				if ua != nil {
					updatedAt = ua
				}
				if rv != nil {
					rowVersion = rv
				}

				if id_ != nil {
					id = id_
				}

			} else {
				if sf.IsNil() {
					panic("Non initialised embedded structure: " + sf.String())
				}
				*level++
				r, ua, rv, id_ := t.fieldAddrsUpdate_(sf.Interface(), cstags, rule, level)
				*level--
				res = append(res, r...)
				if ua != nil {
					updatedAt = ua
				}
				if rv != nil {
					rowVersion = rv
				}

				if id_ != nil {
					id = id_
				}
			}
			continue
		}

		tag := tf.Tag.Get(FieldTagLabel)

		switch {
		case tag == "-":
			continue
		case tf.Name == "CreatedAt":
			continue
		case tf.Name == "ID":
			id = sf.Addr().Interface()
			continue
		case tf.Name == "RowVersion":
			rowVersion = sf.Addr().Interface().(*int)
			continue
		case tf.Name == "DeletedAt":
			continue
		case tf.Name == "UpdatedAt":
			updatedAt = sf.Addr().Interface().(*NullTime)
			//if !updatedAt.Valid() {
			updatedAt.SetNow() // !
			res = append(res, sf.Addr().Interface())
			continue
		}

		switch rule {
		case Exclude:
			if t.anyTagContains(tf.Name, cstags) == true {
				continue
			}
		case Include:
			if t.anyTagContains(tf.Name, cstags) == false {
				continue
			}
		case All:
			break
		default:
			panic("unknown tag exclusion rule")
		}
		//fmt.Printf("%s=%v\n", tf.Name, sf.Addr().Interface())

		res = append(res, sf.Addr().Interface())
	}
	if id != nil && *level == 0 {
		res = append(res, id)
	}
	return
}

func (t *Table) fieldAddrsSelect(model interface{}, cstags string, rule TagExclusionRule) []interface{} {

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
		if tag == "-" {
			continue
		}

		if tf.Anonymous {
			if tf.Type.Kind() != reflect.Ptr {
				res = append(res, t.fieldAddrsSelect(sf.Addr().Interface(), cstags, rule)...)
			} else {
				if sf.IsNil() {
					panic("Non initialised embedded structure: " + sf.String())
				}
				res = append(res, t.fieldAddrsSelect(sf.Interface(), cstags, rule)...)
			}
			continue
		}

		switch rule {
		case Exclude:
			if t.anyTagContains(tf.Name, cstags) == true {
				continue
			}
		case Include:
			if t.anyTagContains(tf.Name, cstags) == false {
				continue
			}
		case All:
			break
		default:
			panic("unknown tag exclusion rule")
		}
		res = append(res, sf.Addr().Interface())
	}
	return res
}
