// Code generated by "enumer -type=densityType -trimprefix=densityType -text -transform=kebab -output enums_densitytype.go"; DO NOT EDIT.

package main

import (
	"fmt"
)

const _densityTypeName = "linearzipf"

var _densityTypeIndex = [...]uint8{0, 6, 10}

func (i densityType) String() string {
	if i < 0 || i >= densityType(len(_densityTypeIndex)-1) {
		return fmt.Sprintf("densityType(%d)", i)
	}
	return _densityTypeName[_densityTypeIndex[i]:_densityTypeIndex[i+1]]
}

var _densityTypeValues = []densityType{0, 1}

var _densityTypeNameToValueMap = map[string]densityType{
	_densityTypeName[0:6]:  0,
	_densityTypeName[6:10]: 1,
}

// densityTypeString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func densityTypeString(s string) (densityType, error) {
	if val, ok := _densityTypeNameToValueMap[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to densityType values", s)
}

// densityTypeValues returns all values of the enum
func densityTypeValues() []densityType {
	return _densityTypeValues
}

// IsAdensityType returns "true" if the value is listed in the enum definition. "false" otherwise
func (i densityType) IsAdensityType() bool {
	for _, v := range _densityTypeValues {
		if i == v {
			return true
		}
	}
	return false
}

// MarshalText implements the encoding.TextMarshaler interface for densityType
func (i densityType) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface for densityType
func (i *densityType) UnmarshalText(text []byte) error {
	var err error
	*i, err = densityTypeString(string(text))
	return err
}
