/*
   Copyright © 2021-2022 The CDI Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cdi

import (
	"strings"

	"github.com/pkg/errors"
)

const (
	// AnnotationPrefix is the prefix for CDI container annotation keys.
	AnnotationPrefix = "cdi.k8s.io/"
)

// AnnotateInjection updates annotations with a plugin-specific CDI device
// injection request for the given devices. Upon any error a non-nil error
// is returned and annotations are left intact. By convention plugin should
// be in the format of "vendor.device-type".
func AnnotateInjection(annotations map[string]string, plugin string, deviceID string, devices []string) (map[string]string, error) {
	key, err := AnnotationKey(plugin, deviceID)
	if err != nil {
		return annotations, errors.Wrap(err, "CDI annotation failed")
	}
	if _, ok := annotations[key]; ok {
		return annotations, errors.Errorf("CDI annotation failed, key %q used", key)
	}
	value, err := AnnotationValue(devices)
	if err != nil {
		return annotations, errors.Wrap(err, "CDI annotation failed")
	}

	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value

	return annotations, nil
}

// ParsePodAnnotations is a temporary kludge to allow testing
// annotations without a K8s DP. Will be removed from final PR.
// XXX TODO remove this eventually...
func ParsePodAnnotations(name string, annotations map[string]string) ([]string, error) {
	prefix := name + "." + AnnotationPrefix
	matching := map[string]string{}
	for key, value := range annotations {
		if strings.HasPrefix(key, prefix) {
			matching[strings.TrimPrefix(key, name+".")] = value
		}
	}
	return ParseAnnotations(matching)
}

// ParseAnnotations parses annotations for CDI device injection requests.
// The devices from all such requests are collected into a slice which is
// returned as the result. All devices are expected to be fully qualified
// CDI device names. If any device fails this check an empty result slice
// and an error is returned. The annotations are expected to be formatted
// by or in a compatible fashion to AnnotateInjection().
func ParseAnnotations(annotations map[string]string) ([]string, error) {
	var devices []string

	for key, value := range annotations {
		if strings.HasPrefix(key, AnnotationPrefix) {
			for _, d := range strings.Split(value, ",") {
				if _, _, _, err := ParseQualifiedName(d); err != nil {
					return nil, err
				}
				devices = append(devices, d)
			}
		}
	}

	return devices, nil
}

// AnnotationKey returns a unique annotation key for a K8s device plugin.
// plugin should be in the format of "vendor.device-type". deviceID is the
// deviceID passed to the device plugin for the allocation.
func AnnotationKey(plugin, deviceID string) (string, error) {
	const maxNameLen = 63

	if plugin == "" {
		return "", errors.New("invalid plugin name, empty")
	}
	if deviceID == "" {
		return "", errors.New("invalid deviceID, empty")
	}

	name := plugin + "_" + deviceID

	if len(name) > maxNameLen {
		return "", errors.Errorf("invalid plugin+deviceID %q, too long", name)
	}

	if c := rune(name[0]); !isAlphaNumeric(c) {
		return "", errors.Errorf("invalid name %q, first '%c' should be alphanumeric",
			name, c)
	}
	if len(name) > 2 {
		for _, c := range name[1 : len(name)-1] {
			switch {
			case isAlphaNumeric(c):
			case c == '_' || c == '-' || c == '.':
			default:
				return "", errors.Errorf("invalid name %q, invalid charcter '%c'",
					name, c)
			}
		}
	}
	if c := rune(name[len(name)-1]); !isAlphaNumeric(c) {
		return "", errors.Errorf("invalid name %q, last '%c' should be alphanumeric",
			name, c)
	}

	return AnnotationPrefix + name, nil
}

// AnnotationValue returns an annotation value for the given devices.
func AnnotationValue(devices []string) (string, error) {
	value, sep := "", ""
	for _, d := range devices {
		if _, _, _, err := ParseQualifiedName(d); err != nil {
			return "", err
		}
		value += sep + d
		sep = ","
	}

	return value, nil
}
