package config

import (
	"fmt"
	"io/ioutil"
	"math"
	"reflect"

	"github.com/samwooo/bolsa/gadgets/util"
	"gopkg.in/yaml.v2"
)

type Config struct {
	SSM      SSM      `yaml:"ssm"`
	Web      Web      `yaml:"web"`
	Job      Job      `yaml:"job"`
	Log      Log      `yaml:"log"`
	Queue    Queue    `yaml:"queue"`
	Mongo    Mongo    `yaml:"mongo"`
	MySql    MySql    `yaml:"mysql"`
	Postgres Postgres `yaml:"postgres"`
}

func (c *Config) String() string {
	return util.Stringify(*c)
}

func (c *Config) finalize(v interface{}, h Helper, howManyFields int) {
	for i := 0; i < int(math.Min(float64(howManyFields), float64(reflect.TypeOf(v).NumField()))); i++ {
		if reflect.ValueOf(v).Field(i).Kind() != reflect.String {
			continue
		} else {
			if s, err := h.GetParameter(reflect.ValueOf(v).Field(i).String()); err != nil {
				panic(err)
			} else {
				ps := reflect.ValueOf(&v).Elem()
				if ps.Kind() == reflect.Struct {
					f := ps.Field(i)
					if f.IsValid() && f.CanSet() {
						f.SetString(s)
					}
				}
			}
		}
	}
}

func (c *Config) validate(env string) []error {
	printErrors := func(errors []error) {
		errorString := "Config error: "
		for _, err := range errors {
			errorString += "\n\t" + err.Error()
		}
		fmt.Println(errorString)
	}

	errors := util.Validate(*c)
	helper := newHelper(env, &c.SSM)
	if helper == nil {
		errors = append(errors, fmt.Errorf("initialise SSM failed"))
	}
	if len(errors) > 0 {
		printErrors(errors)
		return errors
	}
	needFinalized := map[int]int{4: 2, 5: 2, 6: 5, 7: 5}
	for k, v := range needFinalized {
		if k >= reflect.TypeOf(*c).NumField() {
			fmt.Println("Field index out of range - ", k)
			continue
		}
		c.finalize(reflect.ValueOf(*c).Field(k).Interface(), helper, v)
	}
	return nil
}

func LoadConfig(env string, configFile string) (*Config, []error) {
	if raw, err := ioutil.ReadFile(configFile); err != nil {
		fmt.Println(err.Error())
		return nil, []error{err}
	} else {
		var appConfigs map[string]map[string]*Config
		if err := yaml.Unmarshal(raw, &appConfigs); err != nil {
			panic(err)
		} else {
			cs, ok := appConfigs[util.APP_NAME]
			if !ok {
				panic(fmt.Errorf("ensure config is for %s", util.APP_NAME))
			} else {
				c, ok := cs[env]
				if !ok {
					panic(fmt.Errorf("missing config for %s", env))
				} else {
					if errors := c.validate(env); len(errors) > 0 {
						return nil, errors
					} else {
						return c, nil
					}
				}
			}
		}
	}
}
