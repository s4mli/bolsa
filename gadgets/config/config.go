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

func (c *Config) finalize(fieldIndex int, howManyFields int, h Helper) {
	parent := reflect.ValueOf(c).Elem().Field(fieldIndex)
	for i := 0; i < howManyFields; i++ {
		field := parent.Field(i)
		if field.Kind() != reflect.String {
			continue
		}
		if s, err := h.GetParameter(field.String()); err != nil {
			panic(err)
		} else {
			if field.IsValid() && field.CanSet() {
				field.SetString(s)
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
		c.finalize(k, v, helper)
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
