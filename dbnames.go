package dbnames

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type BD int

const (
	BDOK BD = iota
	BDERRORINIT
	BDERRORLINK
	BDERRORPARAM
	BDERRORUSERNOTFOUND
	BDERRORUSEREXIST
	BDERRORIP
)

// эта часть файла помогает парсить результат из базы

func FormatTime(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// DBResult описание группы серверов
type DBResult struct {
	results  *sql.Rows
	scanArgs []interface{}
	values   []sql.RawBytes
	names    map[string]int
}

func New(results *sql.Rows) (*DBResult, error) {
	columns, err := results.Columns()
	if err != nil {
		return nil, err
	}
	names := make(map[string]int)
	for i, column := range columns {
		names[column] = i
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	return &DBResult{results: results, scanArgs: scanArgs, values: values, names: names}, nil
}

func (res *DBResult) Scan() error {
	return res.results.Scan(res.scanArgs...)
}

func (res *DBResult) Next() bool {
	return res.results.Next()
}

/*
 * внутрення функция пакета, выдает номер столбца
 */
func (res *DBResult) getInt(ind interface{}) int {
	index := -1
	switch ind.(type) {
	case string:
		{
			i, ok := res.names[ind.(string)]
			if ok {
				index = i
			}
		}
	case int, int8, int16, int32, int64:
		index = int(reflect.ValueOf(ind).Int())
	case uint, uint8, uint16, uint32, uint64:
		index = int(reflect.ValueOf(ind).Uint())
	}
	return index
}

func (res *DBResult) ParseStringPtr(ind interface{}) *string {
	index := res.getInt(ind)
	if index < 0 || index >= len(res.values) {
		return nil
	}
	if res.values[index] == nil {
		return nil
	}
	str := string(res.values[index])
	return &str
}

func (res *DBResult) ParseString(ind interface{}) string {
	ptr := res.ParseStringPtr(ind)
	if ptr == nil {
		return ""
	}
	return *ptr
}

func (res *DBResult) ParseUint32Ptr(ind interface{}) *uint32 {
	index := res.getInt(ind)
	if index < 0 || index >= len(res.values) {
		return nil
	}
	if res.values[index] == nil {
		return nil
	}
	u, err := strconv.ParseInt(string(res.values[index]), 10, 64)
	if err != nil {
		return nil
	}
	out := uint32(u)
	return &out
}

func (res *DBResult) ParseUint32(ind interface{}) uint32 {
	ptr := res.ParseUint32Ptr(ind)
	if ptr == nil {
		return 0
	}
	return *ptr
}

func (res *DBResult) ParseIntPtr(ind interface{}) *int {
	index := res.getInt(ind)
	if index < 0 || index >= len(res.values) {
		return nil
	}
	if res.values[index] == nil {
		return nil
	}
	i, err := strconv.Atoi(string(res.values[index]))
	if err != nil {
		return nil
	}
	return &i
}

func (res *DBResult) ParseInt(ind interface{}) int {
	ptr := res.ParseIntPtr(ind)
	if ptr == nil {
		return 0
	}
	return *ptr
}

func (res *DBResult) ParseTimePtr(ind interface{}) *time.Time {
	index := res.getInt(ind)
	if index < 0 || index >= len(res.values) {
		return nil
	}
	if res.values[index] == nil {
		return nil
	}
	t, err := time.ParseInLocation(time.RFC3339, string(res.values[index]), time.Now().Location())
	if err != nil {
		return nil
	}
	return &t
}

func (res *DBResult) ParseTime(ind interface{}) time.Time {
	ptr := res.ParseTimePtr(ind)
	if ptr == nil {
		return time.Unix(0, 0)
	}
	return *ptr
}

func (res *DBResult) GetRawBytes(ind interface{}) (sql.RawBytes, error) {
	index := res.getInt(ind)
	if index < 0 || index >= len(res.values) {
		return nil, fmt.Errorf("out of range")
	}
	return res.values[index], nil
}

// эта часть помогает формировать запрос

type MYSQLDATETIME time.Time

func (t MYSQLDATETIME) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *MYSQLDATETIME) IsNULL() bool {
	zero := MYSQLDATETIME{}
	if zero == *t {
		return true
	} else {
		return false
	}
}

func (t *MYSQLDATETIME) ToString() string {
	var res string
	if t.IsNULL() {
		res = "NOW()"
	} else {
		res = fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d'",
			(*time.Time)(t).Year(), (*time.Time)(t).Month(), (*time.Time)(t).Day(),
			(*time.Time)(t).Hour(), (*time.Time)(t).Minute(), (*time.Time)(t).Second())
	}
	return res
}

func (t *MYSQLDATETIME) ToStringNULL() string {
	var res string
	if t.IsNULL() {
		res = "NULL"
	} else {
		res = fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d'",
			(*time.Time)(t).Year(), (*time.Time)(t).Month(), (*time.Time)(t).Day(),
			(*time.Time)(t).Hour(), (*time.Time)(t).Minute(), (*time.Time)(t).Second())
	}
	return res
}

func (t *MYSQLDATETIME) FromString(str string) bool {
	if len(str) == 0 {
		*t = MYSQLDATETIME(time.Time{})
		return false
	}
	var year, month, day, hour, min, sec int
	cnt, err := fmt.Sscanf(str, "%04d-%02d-%02d %02d:%02d:%02d", &year, &month, &day, &hour, &min, &sec)
	if err != nil {
		*t = MYSQLDATETIME(time.Time{})
		return false
	}
	if cnt == 6 {
		*t = MYSQLDATETIME(time.Date(year, time.Month(month), day, hour, min, sec, 0, time.Now().Location()))
		return true
	} else if cnt == 3 {
		*t = MYSQLDATETIME(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Now().Location()))
		return true
	}
	*t = MYSQLDATETIME(time.Time{})
	return false
}

func (t *MYSQLDATETIME) Unix() int64 {
	if t.IsNULL() {
		return 0
	}
	return (*time.Time)(t).Unix()
}

func (t *MYSQLDATETIME) Zone() (name string, offset int) {
	if t.IsNULL() {
		return time.Time{}.Zone()
	}
	return (*time.Time)(t).Zone()
}

/*
функция проходит по всем полям структуры equal (туда передается структура а не указатель)
и создает слайс с элементами - названиями полей в запросе: `table`.`name` где
table - имя таблицы передается в функцию
name это название тега db для побличного поля структуры

	type VideoCallRating struct {
		Crc    uint32 `db:"crc"`
		Rating uint32 `db:"rating"`
		Count  uint32 `db:"count"`
	}

если мы никак не ограничиваем число полей в результате то более ничего не передаем
если же мы хотим как то уточнить поля то стоит передавать их тег db в конце например
"crc", "count"
*/
func BuildFields(table string, equal interface{}, fields ...string) []string {
	fullFields := make([]string, 0)
	ct := reflect.TypeOf(equal)
	for i := 0; i < ct.NumField(); i++ {
		field := ct.Field(i)
		if dbFieldName, find := field.Tag.Lookup("db"); find {
			if len(fields) > 0 {
				// если что-то передали то проверяем если нет то все поля берем
				notFound := false
				for _, f := range fields {
					if dbFieldName == f {
						notFound = true
						break
					}
				}
				if !notFound {
					continue
				}
			}
			var fullField string
			if len(table) > 0 {
				fullField = fmt.Sprintf("`%s`.`%s`", table, dbFieldName)
			} else {
				fullField = fmt.Sprintf("`%s`", dbFieldName)
			}
			fullFields = append(fullFields, fullField)
		}
	}
	return fullFields
}

/*
функция аналогична BuildFields но результат будет в том же порядке что и переданые fields если они найдены
*/
func BuildSortFields(table string, equal interface{}, fields ...string) []string {
	fullFields := BuildFields(table, equal, fields...)
	if len(fields) == 0 || len(fullFields) == 0 {
		return fullFields
	}
	insert := 0
	for _, f := range fields {
		found := -1
		fullField := ""
		if len(table) > 0 {
			fullField = fmt.Sprintf("`%s`.`%s`", table, f)
		} else {
			fullField = fmt.Sprintf("`%s`", f)
		}
		for ffi, ff := range fullFields {
			if fullField == ff {
				found = ffi
			}
		}
		if found != -1 {
			if found != insert {
				tmp := fullFields[insert]
				fullFields[insert] = fullFields[found]
				fullFields[found] = tmp
			}
			insert += 1
		}
	}
	return fullFields
}

/*
тип для формирования условия в запросе
*/
type Operation int

const (
	UNDEF Operation = iota
	ISNULL
	ISNOTNULL
	EQUAL
	NOTEQ
	LESS
	LESSEQ
	MORE
	MOREQE
	IN
	NOTIN
)

func (oper Operation) ToString(condition string) string {
	res := ""
	switch oper {
	case ISNULL:
		res = " IS NULL"
	case ISNOTNULL:
		res = " IS NOT NULL"
	case EQUAL:
		res = fmt.Sprintf("=%s", condition)
	case NOTEQ:
		res = fmt.Sprintf("!=%s", condition)
	case LESS:
		res = fmt.Sprintf("<%s", condition)
	case LESSEQ:
		res = fmt.Sprintf("<=%s", condition)
	case MORE:
		res = fmt.Sprintf(">%s", condition)
	case MOREQE:
		res = fmt.Sprintf(">=%s", condition)
	case IN:
		res = fmt.Sprintf(" IN (%s)", condition)
	case NOTIN:
		res = fmt.Sprintf(" NOT IN (%s)", condition)
	}
	return res
}

/*
обычно для передачи значений хватате чисел и строк но бывает исключения
например sql тип datetime - поэтому объявили интерфейс и все хитрые типы/струтуры
могут его имплементировать, как например MYSQLDATETIME.
*/
type ToStringInteface interface {
	ToString() string
}

type FromStringInteface interface {
	FromString(str string) bool
}

/*
Эта функция генерирует условия для блока WHERE в sql запросе см функцию BuildFields.
Она проходит по всем публичным полям структуры fields и если поле содержит тег db и
оно не пустое то в массив результат генерируется условие для этого поля с оператором
operation, но само сравнение происходит с объектом data (оно должно быть такого же типа
что и поле структуры fields) - сделано так потому что иногда и 0 нужно передать как условие.
Обычно генерирутся ОДНО условие, но если по логике нужно одно и то же условие для разных
полей то можно сгенерировать эти строки за 1 раз: например first=1122, second=1122
если поставить между ними ADN - получим условие с кем разговаривал юзер 1122
(неважно в какой роли звонящий или вызываемый).
Данная функция не проставляет AND OR или скобки она лишь генерирует базовые фильтры для sql запроса
*/
func BuildCondition(table string, fields interface{}, operation Operation, data interface{}) []string {
	fillCond := make([]string, 0)
	fieldsType := reflect.TypeOf(fields)
	fieldsValue := reflect.ValueOf(fields)
	toStringType := reflect.TypeOf((*ToStringInteface)(nil)).Elem()
	for i := 0; i < fieldsType.NumField(); i++ {
		fieldType := fieldsType.Field(i)
		if dbFieldName, find := fieldType.Tag.Lookup("db"); find {
			var fullField string
			if len(table) > 0 {
				fullField = fmt.Sprintf("`%s`.`%s`", table, dbFieldName)
			} else {
				fullField = fmt.Sprintf("`%s`", dbFieldName)
			}
			fieldValue := fieldsValue.Field(i)
			kindValue := fieldValue.Kind()
			switch kindValue {
			case reflect.String:
				if len(fieldValue.String()) > 0 {
					s := strings.ReplaceAll(fmt.Sprintf("%v", data), "'", "\\'")
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("'%s'", s)))
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if fieldValue.Int() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%v", data)))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if fieldValue.Uint() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%v", data)))
				}
			case reflect.Float32, reflect.Float64:
				if fieldValue.Float() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%v", data)))
				}
			default:
				/*
					fields.field := SomeStruct{}
					func (t *SomeStruct) ToString() string {
						return "..."
					}
				*/
				if fieldsValue.IsValid() && fieldType.Type == reflect.TypeOf(data) && !fieldValue.IsZero() && reflect.PointerTo(reflect.TypeOf(data)).Implements(toStringType) {
					pdata := reflect.New(reflect.TypeOf(data))
					pdata.Elem().Set(reflect.ValueOf(data))
					tc := pdata.MethodByName("ToString")
					if tc.IsValid() {
						arr := tc.Call([]reflect.Value{})
						if len(arr) == 1 {
							fillCond = append(fillCond, " "+fullField+operation.ToString(arr[0].String()))
						}
					}
				}
			}
		}
	}
	return fillCond
}

/*
см функцию BuildCondition. Эта может создавать разные условия для разных полей, но с
одним оператором сравнения, еще одно ограничение сравнение с нулевым полем пропускаются
обычно operation это операция сравнения.
*/
func BuildConditions(table string, fields interface{}, operation Operation) []string {
	fillCond := make([]string, 0)
	fieldsType := reflect.TypeOf(fields)
	fieldsValue := reflect.ValueOf(fields)
	toStringType := reflect.TypeOf((*ToStringInteface)(nil)).Elem()
	for i := 0; i < fieldsType.NumField(); i++ {
		fieldType := fieldsType.Field(i)
		if dbFieldName, find := fieldType.Tag.Lookup("db"); find {
			var fullField string
			if len(table) > 0 {
				fullField = fmt.Sprintf("`%s`.`%s`", table, dbFieldName)
			} else {
				fullField = fmt.Sprintf("`%s`", dbFieldName)
			}
			fieldValue := fieldsValue.Field(i)
			kindValue := fieldValue.Kind()
			switch kindValue {
			case reflect.String:
				if len(fieldValue.String()) > 0 {
					s := strings.ReplaceAll(fieldValue.String(), "'", "\\'")
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("'%s'", s)))
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if fieldValue.Int() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%d", fieldValue.Int())))
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if fieldValue.Uint() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%d", fieldValue.Uint())))
				}
			case reflect.Float32, reflect.Float64:
				if fieldValue.Float() != 0 {
					fillCond = append(fillCond, " "+fullField+operation.ToString(fmt.Sprintf("%f", fieldValue.Float())))
				}
			default:
				/*
					data := SomeStruct{}
					func (t *SomeStruct) ToString() string {
						return "..."
					}
				*/
				if fieldsValue.IsValid() && !fieldValue.IsZero() && reflect.PointerTo(fieldType.Type).Implements(toStringType) {
					pdata := reflect.New(fieldType.Type)
					pdata.Elem().Set(fieldValue)
					tc := pdata.MethodByName("ToString")
					if tc.IsValid() {
						arr := tc.Call([]reflect.Value{})
						if len(arr) == 1 {
							fillCond = append(fillCond, " "+fullField+operation.ToString(arr[0].String()))
						}
					}
				}
			}
		}
	}
	return fillCond
}

// эта часть заполняет поля структуры из запроса, продвинутый парсинг результата

/*
функция заполняет структуру по указателю data на основании данных из
БД в объекте result, который содержит *sql.Rows мап с названием столбцов
возвращает число заполненных полей
*/
func FillByDBResult(result *DBResult, data interface{}) int {
	count := 0
	fieldsValue := reflect.ValueOf(data).Elem()
	fieldsType := fieldsValue.Type()
	fromStringType := reflect.TypeOf((*FromStringInteface)(nil)).Elem()
	for i := 0; i < fieldsType.NumField(); i++ {
		fieldType := fieldsType.Field(i)
		if dbFieldName, find := fieldType.Tag.Lookup("db"); find {
			valStr := result.ParseStringPtr(dbFieldName)
			if valStr == nil {
				continue
			}
			fieldValue := fieldsValue.Field(i)
			kindValue := fieldValue.Kind()
			if !fieldValue.CanSet() {
				continue
			}

			switch kindValue {
			case reflect.String:
				fieldValue.SetString(*valStr)
				count += 1
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if tmp, err := strconv.ParseInt(*valStr, 10, 64); err == nil {
					fieldValue.SetInt(tmp)
					count += 1
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if tmp, err := strconv.ParseUint(*valStr, 10, 64); err == nil {
					fieldValue.SetUint(tmp)
					count += 1
				}
			case reflect.Float32, reflect.Float64:
				if tmp, err := strconv.ParseFloat(*valStr, 64); err == nil {
					fieldValue.SetFloat(tmp)
					count += 1
				}
			default:
				if fieldValue.IsValid() && reflect.PointerTo(fieldType.Type).Implements(fromStringType) {
					ts, ok := fieldValue.Addr().Interface().(FromStringInteface)
					if ok {
						ts.FromString(*valStr)
						count += 1
					}
				}
			}
		}
	}
	return count
}
