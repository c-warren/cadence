package nosqlplugin

import (
	"os"
	"strings"
)

func getCadencePackageDir() (string, error) {
	cadencePackageDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cadenceIndex := strings.LastIndex(cadencePackageDir, "cadence/")
	cadencePackageDir = cadencePackageDir[:cadenceIndex+len("cadence/")]
	return cadencePackageDir, err
}

func GetDefaultTestSchemaDir(testSchemaRelativePath string) (string, error) {
	cadencePackageDir, err := getCadencePackageDir()
	if err != nil {
		return "", err
	}
	return cadencePackageDir + testSchemaRelativePath, nil
}
