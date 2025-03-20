package dbnames

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Date struct {
	Time    MYSQLDATETIME `json:"time", omitempty`
	TimeStr string        `json:"timeStr"`
}

func TestMYSQLDATETIMEJson(t *testing.T) {
	obj := Date{Time: MYSQLDATETIME{}}
	obj.TimeStr = obj.Time.ToString()
	b1, err1 := json.Marshal(obj)
	if err1 != nil {
		t.Error(err1)
	}
	str1 := string(b1)
	t.Log(str1)
	custom := MYSQLDATETIME{}
	custom.FromString("2023-05-12 21:41:23")
	obj.Time = custom
	obj.TimeStr = obj.Time.ToString()
	b2, err2 := json.Marshal(obj)
	if err2 != nil {
		t.Error(err2)
	}
	str2 := string(b2)
	t.Log(str2)

	js, errM := json.Marshal(custom)
	if errM != nil {
		t.Error(errM)
	}
	check := MYSQLDATETIME{}
	if errU := json.Unmarshal(js, &check); errU != nil {
		t.Error(errU)
	} else if custom.ToString() != check.ToString() {
		t.Error("bad json serialization")
	} else {
		t.Log(check.ToString())
		t.Log(custom.ToString())
	}
}

func TestMYSQLDATETIME(t *testing.T) {

	zoneName := ""
	zoneOffset := 0
	custom := MYSQLDATETIME{}
	custom.FromString("2023-05-12 21:41:23")
	if custom.ToString() != "'2023-05-12 21:41:23'" {
		t.Errorf("FromString not work")
	}
	if custom.ToStringNULL() != "'2023-05-12 21:41:23'" {
		t.Errorf("FromString not work")
	}
	customUnix := custom.Unix()
	t.Log("2023-05-12 21:41:23 as UNIX is", customUnix)
	zoneName, zoneOffset = custom.Zone()
	t.Log("2023-05-12 21:41:23 Zone is", zoneName, zoneOffset)

	null := MYSQLDATETIME{}
	t.Log("null", null)
	if !null.IsNULL() {
		t.Errorf("now isn't NULL")
	}
	nullStr := null.ToString()
	if nullStr != "NOW()" {
		t.Errorf("null has string representation")
	}
	nullStrNULL := null.ToStringNULL()
	if nullStrNULL != "NULL" {
		t.Errorf("null has string representation")
	}
	nullUnix := null.Unix()
	t.Log("null time as UNIX is", nullUnix)
	zoneName, zoneOffset = null.Zone()
	t.Log("null time Zone is", zoneName, zoneOffset)

	check := MYSQLDATETIME(time.Time{})
	t.Log("check", check.ToString())
	if !check.IsNULL() {
		t.Errorf("null isn't NULL")
	}
	checkStr := check.ToString()
	if checkStr != "NOW()" {
		t.Errorf("check hasn't string NOW()")
	}
	checkStrNULL := null.ToStringNULL()
	if checkStrNULL != "NULL" {
		t.Errorf("check has string representation")
	}
	checkUnix := null.Unix()
	t.Log("check time as UNIX is", checkUnix)
	zoneName, zoneOffset = custom.Zone()
	t.Log("check time Zone is", zoneName, zoneOffset)

	now := MYSQLDATETIME(time.Now())
	t.Log("now", now.ToString())
	if now.IsNULL() {
		t.Errorf("now isn't NULL")
	}
	if now.ToString() == "NOW()" {
		t.Errorf("time.Now() not work")
	}
	if custom.ToStringNULL() == "NULL" {
		t.Errorf("time.Now() not work")
	}
	nowUnix := custom.Unix()
	t.Log("time.Now() as UNIX is", nowUnix)
	zoneName, zoneOffset = now.Zone()
	t.Log("now time Zone is", zoneName, zoneOffset)
}

type DBData struct {
	Crc      uint32        `db:"crc"`
	Create   MYSQLDATETIME `db:"create"`
	Desc     string        `db:"desc"`
	NotFound string
}

func TestDBData(t *testing.T) {
	allFields := BuildFields("some_table", DBData{})
	if len(allFields) != 3 {
		t.Errorf("can't generate all column name")
	}
	t.Log("allFields", allFields)

	definedFields := BuildFields("some_table", DBData{}, "crc", "create", "NotFound")
	if len(definedFields) != 2 {
		t.Errorf("can't generate defined column name")
	}
	t.Log("definedFields", definedFields)

	// по умолчанию сортировка идет как описанно в структуре и как идет по ним рефлексия
	unsortedFields := BuildFields("some_table", DBData{}, "crc", "NotFound", "desc", "create")
	if len(unsortedFields) != 3 {
		t.Errorf("can't generate unsorted column name")
	}
	t.Log("unsortedFields", unsortedFields)

	// но можно изменить сортировку
	sortedFields := BuildSortFields("some_table", DBData{}, "crc", "NotFound", "desc", "create")
	if len(sortedFields) != 3 {
		t.Errorf("can't generate sorted column name")
	}
	if sortedFields[0] == unsortedFields[0] && sortedFields[1] == unsortedFields[1] && sortedFields[2] == unsortedFields[2] {
		t.Errorf("can't generate sorted column name")
	}
	t.Log("sortedFields", sortedFields)

	now := MYSQLDATETIME(time.Now())
	cond1 := make([]string, 0)
	cond1 = append(cond1, BuildCondition("some_table", DBData{Crc: 1}, EQUAL, 10)...)
	cond1 = append(cond1, BuildCondition("some_table", DBData{Create: now}, MORE, now)...)
	cond1 = append(cond1, BuildCondition("some_table", DBData{Desc: "te'st"}, NOTEQ, "te'st")...)
	t.Log("cond1", cond1)

	cond2 := BuildConditions("some_table", DBData{Crc: 10, Create: now, Desc: "te'st"}, EQUAL)
	t.Log("cond2", cond2)
}

type UserIdType uint32

type AuthType int

const (
	UnauthType AuthType = iota
	PhoneType
	EmailType

	_minAuthType = PhoneType
	_maxAuthType = EmailType

	InvalidType = _maxAuthType + 1
)

const DBAuthTable string = "auth"

/*
CREATE TABLE `auth` (
  `user_id` int(10) unsigned NOT NULL,
  `auth_type` tinyint(4) NOT NULL,
  `create` datetime NOT NULL,
  `data` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO `auth` VALUES (184216, 0, NOW(), "{'name': 'Denis'}");

*/

type DBAuth struct {
	UserId UserIdType    `db:"user_id"`
	Type   AuthType      `db:"auth_type"`
	Create MYSQLDATETIME `db:"create"`
	Data   string        `db:"data"`
}

func TestFillFromDBData(t *testing.T) {
	// t.Skip("Need connection to database and created non empty table with defined structure on it")
	// шаблон запроса
	fields := BuildFields(DBAuthTable, DBAuth{}, "user_id", "auth_type", "first_begin", "create", "data")
	query := fmt.Sprintf("SELECT %s FROM `%s` LIMIT 1;", strings.Join(fields, ", "), DBAuthTable)

	db, errOpen := sql.Open("mysql", "test:pass@tcp(127.0.0.1:3306)/mp")
	if errOpen != nil {
		t.Error(errOpen)
	}
	defer db.Close()

	rows, errQuery := db.Query(query)
	if errQuery != nil {
		t.Error(errQuery)
	}
	defer rows.Close()

	res, errdbNames := New(rows)
	if errdbNames != nil {
		t.Error(errdbNames)
	}

	data := DBAuth{}
	initMembers := 0
	count := 0
	for res.Next() {
		if scan := res.Scan(); scan == nil {
			count += 1
			initMembers = FillByDBResult(res, &data)
		}
	}
	if count == 0 {
		t.Errorf("no data into table")
	}
	if initMembers != 4 {
		t.Errorf("not all fields are initialized")
	}
	userId := data.UserId
	authType := data.Type
	value := data.Data
	create := data.Create
	str := create.ToString()
	unix := create.Unix()

	t.Log(userId)
	t.Log(authType)
	t.Log(value)
	t.Log(str)
	t.Log(unix)
}
