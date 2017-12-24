package config

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/samwooo/bolsa/gadgets/util"
)

type Helper interface {
	GetParameter(string) (string, error)
}

/* for dev env */
type devHelper struct{}

func (s *devHelper) GetParameter(key string) (string, error) {
	return key, nil
}

/* ssm */
type ssmHelper struct {
	env string
	svc *ssm.SSM
}

func (s *ssmHelper) GetParameter(key string) (string, error) {
	k := fmt.Sprintf("/%s/%s/%s", s.env, util.APP_NAME, key)
	if output, err := s.svc.GetParameter(&ssm.GetParameterInput{Name: &k}); err != nil {
		return "", err
	} else {
		return *output.Parameter.Value, nil
	}
}

func newHelper(env string, sc *SSM) Helper {
	if strings.ToLower(env) == "development" {
		return &devHelper{}
	} else {
		config := &aws.Config{
			Region:   &sc.Region,
			LogLevel: aws.LogLevel(aws.LogOff),
		}
		if s := session.Must(session.NewSession(config)); s != nil {
			return &ssmHelper{
				env: env,
				svc: ssm.New(s)}
		} else {
			return nil
		}
	}
}
