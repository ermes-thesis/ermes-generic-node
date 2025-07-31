package private

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"server/helper"
	"server/info"
)

func HandleRoutePvtDataTransferRequest(logger *io.Writer, serverInfo *info.ServerInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		fmt.Fprintf(*logger, "got /data-transfer-request")

		// I received the request and it has already been authorized

		// Decode the body

		var body info.SimpleClientIdRequest

		err := helper.DecodeJSONRequestBody(w, r, &body)

		// Badly formatted request

		if err != nil {
			var mr *helper.MalformedRequest
			if errors.As(err, &mr) {
				fmt.Fprintf(*logger, "The request could not be decoded correctly.\n")
				http.Error(w, mr.Msg, mr.Status)
			} else {
				fmt.Fprintln(*logger, err.Error())
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}

			w.WriteHeader(http.StatusBadRequest)

			return
		}

		// TODO: send the data to the data manager, this ok variable is to check if it was possible to store the data

		ok := true

		if ok {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}

	}
}
