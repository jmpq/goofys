// Copyright 2019 Pan Jiaming (jiaming.pan@gmail.com)

package common

import (
	"encoding/json"
	"io/ioutil"
	"net/url"
	"strings"
)

type BackendConfig struct {
	Type      string
	Name      string
	AccessKey string
	SecretKey string
	Endpoint  string
}

type MultiCloudConfig struct {
	Backends []BackendConfig
}

func NewMultiCloudConfig(endpoint string, bucket string) (*MultiCloudConfig, error) {
	if strings.HasPrefix(endpoint, "@") {
		var config MultiCloudConfig

		//load from file
		configFile := strings.TrimPrefix(endpoint, "@")
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(data, &config)
		if err != nil {
			return nil, err
		}

		return &config, nil
	} else {
		var config MultiCloudConfig
		// s1|http://ak:sk@url,s2|http://ak:sk@url,...

		endpoints := strings.Split(endpoint, ",")
		for _, endpoint := range endpoints {
			strs := strings.Split(endpoint, "=")
			if len(strs) < 2 {
				continue
			}
			name := strs[0]
			endpoint = strs[1]

			u, err := url.Parse(endpoint)
			if err != nil {
				continue
			}
			password, _ := u.User.Password()
			c := BackendConfig{
				Type:      "s3",
				Name:      name,
				AccessKey: u.User.Username(),
				SecretKey: password,
				Endpoint:  u.Scheme + "://" + u.Hostname() + ":" + u.Port(),
			}

			config.Backends = append(config.Backends, c)
		}

		return &config, nil
	}
}
