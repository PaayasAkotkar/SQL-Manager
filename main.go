// Package sqlmanager a handlful of microservices that does the job easy for extracting and connecting the db
package sqlmanager

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

// ConnectSQL implments the root of the struct to join
// note: for extracting methods you must passed the type value as an address
type ConnectSQL struct {
	DBname    string
	Tablename string
	ID        any
	Conn      *sql.DB
}

// Init inits the data
// note: as you been providing teh config it will also set the default environment too
// note: id can be the id for the fetched id that you are going to query for
func (conn_ *ConnectSQL) Init(dbname, tablename string, id any, config *mysql.Config) (*ConnectSQL, error) {
	log.SetFlags(log.Ldate | log.Ltime)

	err := os.Setenv(config.User, config.User)
	if err != nil {
		log.Println(errors.New(err.Error()))
		return nil, err
	}

	err = os.Setenv(config.Passwd, config.Passwd)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	_conn, err := sql.Open("mysql", config.FormatDSN())

	if err != nil {
		log.Println(err)
		return nil, err
	}

	conn_.Conn = _conn
	conn_.DBname = dbname
	conn_.Tablename = tablename
	conn_.ID = id

	return conn_, nil
}

func (conn_ *ConnectSQL) DB() *sql.DB {
	return conn_.Conn
}

// ExtractSingleDataFromJSON returns the value of the field
func (conn_ *ConnectSQL) ExtractSingleDataFromJSON(tablename, extractToken, json_ string, token any) error {
	conn := conn_.Conn
	id := conn_.ID
	var exists, err = conn_.HasID(tablename)
	if err != nil {
		log.Println(err)
		return err
	}

	if exists {
		q := fmt.Sprintf(`select JSON_EXTRACT(%s,"$.%s") from %s where id = ?`,
			json_, extractToken, tablename)
		if err := conn.QueryRow(q, id).Scan(token); err != nil {
			log.Println(err)
			return err
		}
	} else {
		log.Println("no client found with id", id)
		return errors.New("invalid client id")
	}
	return nil
}

// PatchNonJSON patches the token based on query
func (conn_ *ConnectSQL) PatchNonJSON(query string, args ...any) error {
	conn := conn_.Conn
	stmt, err := conn.Prepare(query)
	if err != nil {
		log.Println(err)
		return err
	}

	if _, err := stmt.Exec(args...); err != nil {
		log.Println(err)
		return nil
	}

	return nil
}

// Validation returns true if the arg exists as per the query
func (conn_ *ConnectSQL) Validation(query, arg string) (bool, error) {
	var exists bool
	conn := conn_.Conn

	Qexists := query
	if err := conn.QueryRow(Qexists, arg).Scan(&exists); err != nil {
		log.Println(err)
		return exists, err
	}
	return exists, nil
}

// ExtractSingleData returns the one of the valid token
func (conn_ *ConnectSQL) ExtractSingleData(tablename, extractToken string, token any) error {
	id := conn_.ID
	conn := conn_.Conn
	exists, err := conn_.HasID(tablename)
	if err != nil {
		return err
	}
	// Qexists := fmt.Sprintf("select exists (select 1 from %s where id = ?)", tablename)
	// if err := conn.QueryRow(Qexists, id).Scan(&exists); err != nil {
	// 	panic(err)
	// }
	if exists {
		q := fmt.Sprintf("select %s from %s where id = ?", extractToken, tablename)
		if err := conn.QueryRow(q, id).Scan(&token); err != nil {
			log.Println(err)
			return err
		}
	} else {
		log.Println("client not found")
		return err
	}
	return nil
}

// ExtractData returns the struct data into json struct
// json_struct_name must be the struct that is the kind of mysql
func (conn_ *ConnectSQL) ExtractData(jsonStructName, tablename string, src any) error {
	id := conn_.ID
	var exe []byte
	exists, err := conn_.HasID(tablename)

	if err != nil {
		log.Println(err)
		return err
	}
	conn := conn_.Conn

	// Qexists := fmt.Sprintf("select exists (select 1 from %s where id =?)", tableName)
	// if err := conn.QueryRow(Qexists, id).Scan(&exists); err != nil {
	// 	panic(err)
	// }

	if exists {
		fmt.Println("exists")
		query := fmt.Sprintf("select %s from %s where id =?", jsonStructName, tablename)

		if err := conn.QueryRow(query, id).Scan(&exe); err != nil {
			log.Println(err)
			return err
		}
		fmt.Println("ss: ", string(exe))
		if err := json.Unmarshal(exe, src); err != nil {
			log.Println(err)
			return err
		}
	} else {
		log.Println(err)
		return err
	}
	fmt.Println("done extracting")
	return nil
}

// HasID returns true if exists
// note: id either can be in string or binary
func (conn_ *ConnectSQL) HasID(tablename string) (bool, error) {
	conn := conn_.Conn
	id := conn_.ID
	var exists bool
	Qexists := fmt.Sprintf("select exists (select 1 from %s where id =?)", tablename)
	if err := conn.QueryRow(Qexists, id).Scan(&exists); err != nil {
		log.Println(err)
		return exists, err
	}
	return exists, nil
}

// CloseDB closes the db
// pro-tip: to use this in a function where you are performing whole query
// func main(){ xxxxx defer c.CloseDB() xxx xxxx }
// what this ensures is that the connection is valid and if in case the
// singal is lost it closes
func (conn_ *ConnectSQL) CloseDB() error {
	return conn_.Conn.Close()
}
