package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func scanSlice(v reflect.Value, tElem reflect.Type, rows *sql.Rows, colsLen int, mysqlFieldTemp, cols []string) (err error) { //对切片的扫描
	ttElem := tElem.Elem()                //获取切片内的元素反射 main.User
	isPtr := ttElem.Kind() == reflect.Ptr //判断切片内的元素是否是指针类型
	if isPtr {
		ttElem = ttElem.Elem() //如果是指针就获取元素本身main.User
	}

	switch ttElem.Kind() { //判断元素的类型
	case reflect.Struct:
		err = scanSliceStruct(v, ttElem, rows, colsLen, mysqlFieldTemp, isPtr)
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
		if colsLen > 1 { //查询结果不止一个字段
			return fmt.Errorf("There are multiple fields,Please receive in []Struct or []Map")
		}
		err = scanSliceBaseType(v, ttElem, rows, isPtr)
	case reflect.Map:
		err = scanSliceMap(v, ttElem, rows, cols, colsLen, isPtr)
	default:
		return fmt.Errorf("%v is Not a Can be formatted type", ttElem.Kind())
	}
	if err != nil {
		return
	}
	return rows.Err()
}
func scanSliceMap(v reflect.Value, ttElem reflect.Type, rows *sql.Rows, cols []string, colsLen int, isPtr bool) (err error) {
	vElem := v.Elem()
	switch ttElem.String() {
	case "map[string]string":
		for rows.Next() {
			rawResult := make([]interface{}, colsLen)
			result := make(map[string]string, colsLen)
			dest := make([]interface{}, colsLen)
			for i, _ := range rawResult {
				dest[i] = &rawResult[i]
			}
			err = rows.Scan(dest...)
			if err != nil {
				return
			}
			for i, raw := range rawResult {
				switch raw.(type) { //获取数据库中类型
				case int64:
					result[cols[i]] = strconv.FormatInt(raw.(int64), 10)
				case []uint8:
					result[cols[i]] = string(raw.([]byte))
				case time.Time:
					result[cols[i]] = raw.(time.Time).Format("2006-01-02 15:04:05")
				case nil:
					result[cols[i]] = ""
				}
			}
			vp := reflect.New(ttElem) ///New返回一个值，该值表示指向指定类型的新零值的指针。也就是说，返回值的类型是PointerTo（typ）
			vpElem := vp.Elem()       // 获取指针所指向的元素
			vpElem.Set(reflect.ValueOf(result))
			if isPtr {
				vElem.Set(reflect.Append(vElem, vp))
			} else {
				vElem.Set(reflect.Append(vElem, vp.Elem()))
			}
		}
	case "map[string]interface {}":
		for rows.Next() {
			rawResult := make([]interface{}, colsLen)
			result := make(map[string]interface{}, colsLen)
			dest := make([]interface{}, colsLen)
			for i, _ := range rawResult {
				dest[i] = &rawResult[i]
			}
			err = rows.Scan(dest...)
			if err != nil {
				return
			}
			for i, raw := range rawResult {
				switch raw.(type) { //获取数据库中类型
				case []uint8:
					result[cols[i]] = string(raw.([]byte))
				default:
					result[cols[i]] = raw
				}
			}
			vp := reflect.New(ttElem) ///New返回一个值，该值表示指向指定类型的新零值的指针。也就是说，返回值的类型是PointerTo（typ）
			vpElem := vp.Elem()       // 获取指针所指向的元素
			vpElem.Set(reflect.ValueOf(result))
			if isPtr {
				vElem.Set(reflect.Append(vElem, vp))
			} else {
				vElem.Set(reflect.Append(vElem, vp.Elem()))
			}
		}
	default:
		return fmt.Errorf("Only use []map[string]string or []map[string]interface{}")
	}
	return
}

func scanSliceBaseType(v reflect.Value, ttElem reflect.Type, rows *sql.Rows, isPtr bool) (err error) {
	vElem := v.Elem()
	for rows.Next() {
		var dest interface{}
		err = rows.Scan(&dest)
		if err != nil {
			return err
		}
		vp := reflect.New(ttElem)                                        ///New返回一个值，该值表示指向指定类型的新零值的指针。也就是说，返回值的类型是PointerTo（typ）
		vpElem := vp.Elem()                                              // 获取指针所指向的元素
		err = colFormatField(ttElem.Name(), dest, vpElem, ttElem.Kind()) //格式化每个字段
		if isPtr {
			vElem.Set(reflect.Append(vElem, vp))
		} else {
			vElem.Set(reflect.Append(vElem, vp.Elem()))
		}
	}
	return
}
func scanSliceStruct(v reflect.Value, ttElem reflect.Type, rows *sql.Rows, colsLen int, mysqlFieldTemp []string, isPtr bool) (err error) {
	structFieldMap := map[string]string{}
	for i := 0; i < ttElem.NumField(); i++ { //遍历所有字段
		key := ttElem.Field(i) // 从0开始获取所包含的字段
		if !key.IsExported() { //不是导出字段（字段首字母没有大写）就不管
			continue
		}
		structFieldMap[strings.ToLower(key.Name)] = key.Name //全部小写，方便比对字段
	}
	//fmt.Println("字段", structFieldMap)
	vElem := v.Elem() // 获取指针指向的元素本身，下面的内容append到此内存中
	var dest []interface{}
	var destTemp []interface{}
	for rows.Next() {
		dest = make([]interface{}, colsLen)
		destTemp = make([]interface{}, colsLen)
		for i, _ := range destTemp {
			dest[i] = &destTemp[i]
		}
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		vp := reflect.New(ttElem) ///New返回一个值，该值表示指向指定类型的新零值的指针。也就是说，返回值的类型是PointerTo（typ）
		vpElem := vp.Elem()       // 获取指针所指向的元素

		for i, value := range destTemp { //遍历一行数据中的所有列
			fieldName, ok := structFieldMap[mysqlFieldTemp[i]] //获取此列名，是否在结构体中也存在此字段
			//fmt.Printf("%d:%T,%v,%s\n", i, value, value, fieldName)
			if !ok { //查出来的字段结构体中不存在
				continue
			}
			field := vpElem.FieldByName(fieldName)                   //获取字段本身
			fieldType := field.Kind()                                //获取字段反射后的类型
			err = colFormatField(fieldName, value, field, fieldType) //格式化每个字段
			if err != nil {
				return
			}
		}
		//fmt.Printf("333 %+v\n", vp)
		if isPtr {
			vElem.Set(reflect.Append(vElem, vp))
		} else {
			vElem.Set(reflect.Append(vElem, vp.Elem()))
		}
	}
	return
}
