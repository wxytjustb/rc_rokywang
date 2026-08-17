package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type errorCode int

const (
	statusOK errorCode = 0

	errInvalidRequest            errorCode = 1001
	errUnauthenticated           errorCode = 1002
	errUnsupportedProviderAction errorCode = 1004
	errInvalidPayload            errorCode = 1005
	errSourceRequestConflict     errorCode = 1006
	errMessageNotFound           errorCode = 1007
	errRouteNotFound             errorCode = 1008
	errMethodNotAllowed          errorCode = 1009
	errStorageUnavailable        errorCode = 2001
	errDependencyUnavailable     errorCode = 2002
	errInternal                  errorCode = 2003
)

type localizedMessage struct {
	en string
	zh string
}

var errorMessages = map[errorCode]localizedMessage{
	errInvalidRequest: {
		en: "The request body is invalid.",
		zh: "请求体格式或参数无效。",
	},
	errUnauthenticated: {
		en: "Authentication failed. Check the bearer token.",
		zh: "身份认证失败，请检查 Bearer Token。",
	},
	errUnsupportedProviderAction: {
		en: "The provider or provider action is not supported.",
		zh: "不支持指定的供应商或供应商动作。",
	},
	errInvalidPayload: {
		en: "The payload does not meet the requirements of this provider action.",
		zh: "Payload 不符合该供应商动作的要求。",
	},
	errSourceRequestConflict: {
		en: "The source request was previously accepted with different content.",
		zh: "该来源请求已使用不同内容提交。",
	},
	errMessageNotFound: {
		en: "No message was found for the source request.",
		zh: "未找到该来源请求对应的消息。",
	},
	errRouteNotFound: {
		en: "The requested API route does not exist.",
		zh: "请求的 API 路径不存在。",
	},
	errMethodNotAllowed: {
		en: "The HTTP method is not allowed for this API route.",
		zh: "该 API 路径不支持此 HTTP 方法。",
	},
	errStorageUnavailable: {
		en: "The storage service is temporarily unavailable.",
		zh: "存储服务暂时不可用。",
	},
	errDependencyUnavailable: {
		en: "A required service dependency is unavailable.",
		zh: "必要的服务依赖当前不可用。",
	},
	errInternal: {
		en: "An internal server error occurred.",
		zh: "服务器内部发生错误。",
	},
}

func writeSuccess(c *gin.Context, httpStatus int, data any) {
	c.JSON(httpStatus, apiResponse{
		Status:       int(statusOK),
		Data:         data,
		ErrorMessage: "",
	})
}

func writeError(c *gin.Context, httpStatus int, code errorCode) {
	errorMessage := localizedErrorMessage(c, code)
	c.AbortWithStatusJSON(httpStatus, apiResponse{
		Status:       int(code),
		Data:         emptyData{},
		ErrorMessage: errorMessage,
	})
}

func writePayloadValidationError(c *gin.Context, problems []string) {
	errorMessage := localizedErrorMessage(c, errInvalidPayload)
	if len(problems) > 0 {
		errorMessage += " " + strings.Join(problems, "; ")
	}
	c.AbortWithStatusJSON(http.StatusUnprocessableEntity, apiResponse{
		Status:       int(errInvalidPayload),
		Data:         emptyData{},
		ErrorMessage: errorMessage,
	})
}

func localizedErrorMessage(c *gin.Context, code errorCode) string {
	message := errorMessages[code]
	if preferredLanguage(c.GetHeader("Accept-Language")) == "zh" {
		return message.zh
	}
	return message.en
}

func preferredLanguage(header string) string {
	bestLanguage := "en"
	bestQuality := -1.0
	for _, item := range strings.Split(header, ",") {
		parts := strings.Split(item, ";")
		language := strings.ToLower(strings.TrimSpace(parts[0]))
		quality := 1.0
		for _, parameter := range parts[1:] {
			parameter = strings.TrimSpace(parameter)
			if !strings.HasPrefix(parameter, "q=") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimPrefix(parameter, "q="), 64)
			if err == nil {
				quality = parsed
			}
		}
		if quality <= bestQuality {
			continue
		}
		switch {
		case language == "zh" || strings.HasPrefix(language, "zh-"):
			bestLanguage = "zh"
			bestQuality = quality
		case language == "en" || strings.HasPrefix(language, "en-") || language == "*":
			bestLanguage = "en"
			bestQuality = quality
		}
	}
	return bestLanguage
}
