package config

type SSM struct {
	Region string `yaml:"region"`
}

type Web struct {
	Port int `yaml:"port"`
}

type Job struct {
	MaxRetries  int `yaml:"maxRetries"`
	RetryDelay  int `yaml:"retryDelay"`
	WorkerCount int `yaml:"workerCount"`
}

type Log struct {
	Level string `yaml:"level"`
}

type Queue struct {
	Region string `yaml:"region"`
	Url    string `yaml:"url"`
	Wait   int    `yaml:"wait"`
}

type MySql struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"db"`
}

type Mongo struct {
	Url    string `yaml:"url"`
	DBName string `yaml:"db"`
}

type Postgres struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"db"`
}
