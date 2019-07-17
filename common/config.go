package common

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"
)

func Validate(v interface{}) []error {
	var errors []error
	gatherError := func(fieldIndex int, supported bool) []error {
		var e error
		if supported {
			e = fmt.Errorf("wrong %s[%s]", reflect.TypeOf(v).Name(),
				reflect.TypeOf(v).Field(fieldIndex).Name)
		} else {
			e = fmt.Errorf("unimplemented type %d for %s", reflect.ValueOf(v).Field(fieldIndex).Kind(),
				strings.ToLower(reflect.TypeOf(v).Field(fieldIndex).Name))
		}
		errors = append(errors, e)
		return errors
	}
	for i := 0; i < reflect.TypeOf(v).NumField(); i++ {
		switch reflect.ValueOf(v).Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			{
				if validate := reflect.ValueOf(v).Field(i).Int() > 0; !validate {
					gatherError(i, true)
				}
			}
		case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
			{
				if validate := reflect.ValueOf(v).Field(i).Len() > 0; !validate {
					gatherError(i, true)
				}
			}
		case reflect.Struct:
			errors = append(errors, Validate(reflect.ValueOf(v).Field(i).Interface())...)
		default:
			gatherError(i, false)
		}
	}
	return errors
}

func Stringify(v interface{}) string {
	s := "\n" + reflect.TypeOf(v).Name()
	for i := 0; i < reflect.TypeOf(v).NumField(); i++ {
		switch reflect.ValueOf(v).Field(i).Kind() {
		case reflect.Struct, reflect.Interface:
			s += Stringify(reflect.ValueOf(v).Field(i).Interface())
		default:
			s += fmt.Sprintf("\n\t%s: %v", reflect.TypeOf(v).Field(i).Name,
				reflect.ValueOf(v).Field(i).Interface())
		}
	}
	return s
}

type Validator interface{ Validate() []error }

func LoadConfig(app, env string, configFile string, config interface{}) {
	if raw, err := ioutil.ReadFile(configFile); err != nil {
		panic(err)
	} else {
		var appConfigs map[string]map[string]interface{}
		if err := yaml.Unmarshal(raw, &appConfigs); err != nil {
			panic(err)
		} else {
			configs, ok := appConfigs[app]
			if !ok {
				panic(fmt.Errorf("ensure config is for %s", app))
			} else {
				if envConfig, ok := configs[env]; !ok {
					panic(fmt.Errorf("missing config for %s", env))
				} else {
					if c, err := yaml.Marshal(envConfig); err != nil {
						panic(err)
					} else {
						if err := yaml.Unmarshal(c, config); err != nil {
							panic(err)
						} else {
							if v, ok := config.(Validator); ok {
								if errs := v.Validate(); len(errs) > 0 {
									panic(ErrorToString(errs))
								}
							}
						}
					}
				}
			}
		}
	}
}
