package v1

import (
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	conf "mall/config"
	"mall/pkg/e"
	"mall/pkg/utils/ctl"
)

func ErrorResponse(ctx *gin.Context, err error) *ctl.TrackedErrorResponse {
	if ve, ok := err.(validator.ValidationErrors); ok {
		for _, fieldError := range ve {
			field := conf.T(fmt.Sprintf("Field.%s", fieldError.Field))
			tag := conf.T(fmt.Sprintf("Tag.Valid.%s", fieldError.Tag))
			return ctl.RespError(ctx, err, fmt.Sprintf("%s%s", field, tag))
		}
	}

	if _, ok := err.(*json.UnmarshalTypeError); ok {
		return ctl.RespError(ctx, err, "JSON类型不匹配")
	}

	return ctl.RespError(ctx, err, "参数错误", e.InvalidParams)
}

func SuccessResponse(ctx *gin.Context, resp interface{}) *ctl.Response {
	return nil
}

func SuccessResponseList(ctx *gin.Context, resp interface{}) *ctl.Response {
	return nil
}
