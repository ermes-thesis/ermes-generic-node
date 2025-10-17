package metadataManager_test

import (
	"bytes"
	"db"
	"db/test_utils"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/propagation"
	"metadataManager/types"
	"mocks/caches"
	"mocks/tokenDecoder"
	"net/http"
	"net/http/httptest"
	"net/url"
	"server"
	"server/info"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

var templates = map[string]model.QueryTemplate{
	"private_data_query": {
		Select: "ALL",
		As:     "private_data",
		Where: &model.ConditionWrapper{Condition: &model.AndCondition{
			Conditions: []model.Condition{
				&model.EqCondition{
					Field: "owner",
					Value: "12345",
				},
				&model.EqCondition{
					Field: "name",
					Value: "<name>",
				},
			},
		}},
	},
	"gt_data_query": {
		Select: "ALL",
		As:     "gt_data",
		Where: &model.ConditionWrapper{Condition: &model.AndCondition{
			Conditions: []model.Condition{
				&model.EqCondition{
					Field: "owner",
					Value: 12345,
				},
				&model.EqCondition{
					Field: "name",
					Value: "DATI_PRIVATE",
				},
				&model.GtCondition{
					Field: "created_at",
					Value: 1745747999,
				},
			},
		}},
	},
}

func createNewLogger(t *testing.T) *logger.Logger {
	log, err := logger.NewLogger(logger.Config{
		Level:     zerolog.DebugLevel,
		Format:    "text",
		AddSource: false,
	})
	require.NoError(t, err)
	return log
}

func TestMetadataLookupIntegration(t *testing.T) {
	redisClient1, shutdownFunc1 := startRedisContainer(t, false)
	defer shutdownFunc1()
	defer redisClient1.Close()

	redisClient2, shutdownFunc2 := startRedisContainer(t, true)
	defer shutdownFunc2()
	defer redisClient2.Close()

	serverInfoFather := &info.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}
	invokedFnName := "test_function"
	invokedFnVer := "1.0.0"
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	q := templates["private_data_query"]
	q2 := templates["gt_data_query"]
	fnCache.EXPECT().GetTemplate(invokedFnName, invokedFnVer, "private_data_query").Return(&q, nil)
	fnCache.EXPECT().GetTemplate(invokedFnName, invokedFnVer, "gt_data_query").Return(&q2, nil)

	tknDecoder := &tokenDecoder_mock.TokenDecoder{}
	tknDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataFatherLog := createNewLogger(t)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClient1, serverInfoFather, metadataFatherLog, fnCache)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	err = metadataManagerFather.RegisterRoutes(serverFather, tknDecoder)
	assert.NoError(t, err, "Expected no error when registering routes")

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			HostName: urlFather.Host,
		},
		Children:       []info.Node{},
		MetadataDomain: "IT/Milan",

		Secret: "secret",
	}
	fnCache2 := &caches_mock.FunctionCache{}

	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "userId", nil)

	metadataChildLog := createNewLogger(t)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClient2, serverInfoChild, metadataChildLog, fnCache2)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	err = metadataManagerChild.RegisterRoutes(serverChild, tknDecoderChild)
	assert.NoError(t, err, "Expected no error when registering routes")

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoFather.Children = []info.Node{
		{
			HostName: urlChild.Host,
		},
	}

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/pvt-lookup", nil)
	assert.NoError(t, err, "Expected no error when creating request")

	queryParams, _ := json.Marshal(map[string]map[string]any{
		"private_data_query": {
			"name": "collectionNameFather",
		},
	})
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-QUERY", string(queryParams))
	req.Header.Set("X-ERMES-GROUPS", tokenString)
	// These headers are needed only for testing purposes. Since we don't have a way to simulate a function
	// invocation request, we have to pass manually the invoked function name and version as headers to make the
	// metadataManager work properly.
	req.Header.Set("X-ERMES-REQ-FUNCTION", invokedFnName)
	req.Header.Set("X-ERMES-REQ-FUNCTION-VERSION", invokedFnVer)

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode, "Expected status code 200 OK")
	bodyResp, err := io.ReadAll(res.Body)

	fmt.Println("Response: " + string(bodyResp))

	assert.NoError(t, err, "Expected no error when reading response body")
	var response map[uint64]types.QueryResult
	err = json.Unmarshal(bodyResp, &response)
	assert.NoError(t, err, "Expected valid JSON response, got error: %v", err)

	assert.Len(t, response, 1, "Expected one collection in response")

	hash := types.NewQueryIdentifier("private_data_query", map[string]string{
		"name": "collectionNameFather",
	}).Hash()
	assert.Contains(t, response, hash, "Expected result to contain 'private_data' key")

	assert.Equal(t, response[hash].As, "private_data", "Expected 'As' field to be 'private_data'")
	assert.Len(t, response[hash].Collections, 1, "Expected 'private_data' to have one collection")
	collection := response[hash].Collections[0]

	assert.Equal(t, "1111", collection.Id, "Expected collection ID to be 1111")
	assert.Equal(t, "12345", collection.Metadata.Owner, "Expected owner to match query parameter")
	assert.Equal(t, "collectionNameFather", collection.Metadata.Name, "Expected name to be 'collectionNameFather'")
	assert.Contains(t, collection.Metadata.Groups, "groupC", "Expected groups to be 'groupC'")

	fmt.Println(response)
}

func TestMetadataCreationFromChildIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	tknDecoder := &tokenDecoder_mock.TokenDecoder{}
	tknDecoder.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataFatherLog := createNewLogger(t)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, metadataFatherLog, fnCache)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	err = metadataManagerFather.RegisterRoutes(serverFather, tknDecoder)
	assert.NoError(t, err, "Expected no error when registering routes")

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			HostName:       urlFather.Host,
			MetadataDomain: "IT",
		},
		Children:       []info.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataChildLog := createNewLogger(t)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, metadataChildLog, fnCacheChild)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	err = metadataManagerChild.RegisterRoutes(serverChild, tknDecoderChild)
	assert.NoError(t, err, "Expected no error when registering routes")

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoFather.Children = []info.Node{
		{
			HostName:       urlChild.Host,
			MetadataDomain: "IT/Milan",
		},
	}

	metadata := model.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    model.EventualConsistent,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}
	jsonMetadata, err := json.Marshal(metadata)
	assert.NoError(t, err, "Expected no error when marshalling metadata")

	req, err := http.NewRequest(http.MethodPost, httpServerChild.URL+"/pvt-create-metadata/12345678", bytes.NewBuffer(jsonMetadata))
	assert.NoError(t, err, "Expected no error when creating request")
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "IT/Milan/test")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode, "Expected status code 200 OK")

	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")
	assert.Empty(t, bodyResp, "Expected empty response body")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Expected metadata to match")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")
}

func TestMetadataCreationFromFatherIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father: &info.Node{
			//root node, so that the father can think that the message arrive from top
		},
		CloudNode:      nil,
		MetadataDomain: "IT",
		Secret:         "secret",
	}
	tokenString := "JWTToken"

	fnCache := &caches_mock.FunctionCache{}
	tknDecoderFather := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataFatherLog := createNewLogger(t)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, metadataFatherLog, fnCache)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	err = metadataManagerFather.RegisterRoutes(serverFather, tknDecoderFather)
	assert.NoError(t, err, "Expected no error when registering routes")

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			HostName:       urlFather.Host,
			MetadataDomain: "IT",
		},
		Children:       []info.Node{},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataChildLog := createNewLogger(t)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, metadataChildLog, fnCacheChild)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = metadataManagerChild.RegisterRoutes(serverChild, tknDecoderChild)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoFather.Children = []info.Node{
		{
			HostName:       urlChild.Host,
			MetadataDomain: "IT/Milan",
		},
	}

	metadata := model.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    model.EventualConsistent,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}
	jsonMetadata, err := json.Marshal(metadata)
	assert.NoError(t, err, "Expected no error when marshalling metadata")

	req, err := http.NewRequest(http.MethodPost, httpServerFather.URL+"/pvt-create-metadata/12345678", bytes.NewBuffer(jsonMetadata))
	assert.NoError(t, err, "Expected no error when creating request")
	req.Header.Set("X-ERMES-SECRET", "secret")
	req.Header.Set("X-ERMES-METADATA-DOMAIN", "")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err, "Expected no error when sending request")
	res.Body.Close()

	assert.Equal(t, http.StatusCreated, res.StatusCode, "Expected status code 200 OK")

	bodyResp, err := io.ReadAll(res.Body)
	assert.NoError(t, err, "Expected no error when reading response body")
	assert.Empty(t, bodyResp, "Expected empty response body")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata, *metaChild, "Expected metadata to match")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")
}

func TestMetadataCreationFromLocalIntegration(t *testing.T) {
	redisClientFather, shutdownFuncFather := startRedisContainer(t, false)
	defer shutdownFuncFather()
	defer redisClientFather.Close()

	redisClientLocal, shutdownFuncLocal := startRedisContainer(t, true)
	defer shutdownFuncLocal()
	defer redisClientLocal.Close()

	redisClientChild, shutdownFuncChild := startRedisContainer(t, true)
	defer shutdownFuncChild()
	defer redisClientChild.Close()

	serverInfoFather := &info.ServerInfo{
		Father:         nil,
		CloudNode:      nil,
		MetadataDomain: "IT",
		Children: []info.Node{
			{
				MetadataDomain: "IT/Milan",
			},
		},
		Secret: "secret",
	}
	tokenString := "JWTToken"

	fnCacheFather := &caches_mock.FunctionCache{}
	tknDecoderFather := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderFather.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataFatherLog := createNewLogger(t)
	metadataManagerFather := metadataManager.NewMetadataManager(redisClientFather, serverInfoFather, metadataFatherLog, fnCacheFather)

	fatherLog := createNewLogger(t)
	serverFather, err := server.NewServer(fatherLog, serverInfoFather)
	assert.NoError(t, err, "Expected no error when creating server")

	httpServerFather := httptest.NewServer(serverFather.Handler)
	defer httpServerFather.Close()

	err = metadataManagerFather.RegisterRoutes(serverFather, tknDecoderFather)
	assert.NoError(t, err, "Expected no error when registering routes")

	serverInfoChild := &info.ServerInfo{
		Father: &info.Node{
			MetadataDomain: "IT/Milan",
		},
		Children:       []info.Node{},
		MetadataDomain: "IT/Milan/Polimi",
		Secret:         "secret",
	}
	fnCacheChild := &caches_mock.FunctionCache{}
	tknDecoderChild := &tokenDecoder_mock.TokenDecoder{}
	tknDecoderChild.EXPECT().DecodeGroupToken(tokenString).Return([]string{"groupC"}, "", nil)

	metadataChildLog := createNewLogger(t)
	metadataManagerChild := metadataManager.NewMetadataManager(redisClientChild, serverInfoChild, metadataChildLog, fnCacheChild)

	childLog := createNewLogger(t)
	serverChild, err := server.NewServer(childLog, serverInfoChild)
	assert.NoError(t, err, "Expected no error when creating server")

	err = metadataManagerChild.RegisterRoutes(serverChild, tknDecoderChild)
	assert.NoError(t, err, "Expected no error when registering routes")

	httpServerChild := httptest.NewServer(serverChild.Handler)
	defer httpServerChild.Close()

	urlFather, err := url.Parse(httpServerFather.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	urlChild, err := url.Parse(httpServerChild.URL)
	assert.NoError(t, err, "Expected no error when parsing URL")

	serverInfoLocal := &info.ServerInfo{
		Father: &info.Node{
			HostName:       urlFather.Host,
			MetadataDomain: "IT",
		},
		Children: []info.Node{
			{
				HostName:       urlChild.Host,
				MetadataDomain: "IT/Milan/Polimi",
			},
		},
		MetadataDomain: "IT/Milan",
		Secret:         "secret",
	}
	fnCacheLocal := &caches_mock.FunctionCache{}

	metadataLocalLog := createNewLogger(t)
	metadataManagerLocal := metadataManager.NewMetadataManager(redisClientLocal, serverInfoLocal, metadataLocalLog, fnCacheLocal)

	metadata := model.Metadata{
		Name:           "test",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    model.EventualConsistent,
		Geolocation:    "IT/Milan",
		CustomMetadata: map[string]string{},
	}

	err = metadataManagerLocal.CreateMetadata("12345678", metadata, propagation.FROM_LOCAL)
	assert.NoError(t, err, "Expected no error when creating metadata from local")

	// Verify that the metadata was created in both father and child
	metaChild, err := redisClientChild.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Nil(t, metaChild, "Expected metadata to be nil in child")

	metaFather, err := redisClientFather.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata, *metaFather, "Expected metadata to match")

	metaLocal, err := redisClientLocal.GetMetadata("12345678")
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata, *metaLocal, "Expected metadata to match")

	metadata2 := model.Metadata{
		Name:           "test2",
		Owner:          "test",
		CreatedAt:      time.Now().Truncate(time.Millisecond),
		Groups:         []string{"groupC"},
		Consistency:    model.EventualConsistent,
		Geolocation:    "IT/Milan/Polimi",
		CustomMetadata: map[string]string{},
	}

	err = metadataManagerLocal.CreateMetadata("test2", metadata2, propagation.FROM_LOCAL)
	assert.NoError(t, err, "Expected no error when creating metadata from local")

	// Verify that the metadata was created in both father and child
	metaChild2, err := redisClientChild.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from child")
	assert.Equal(t, metadata2, *metaChild2, "Expected metadata to match")

	metaFather2, err := redisClientFather.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from father")
	assert.Equal(t, metadata2, *metaFather2, "Expected metadata to match")

	metaLocal2, err := redisClientLocal.GetMetadata("test2")
	assert.NoError(t, err, "Expected no error when getting metadata from local")
	assert.Equal(t, metadata2, *metaLocal2, "Expected metadata to match")
}

func startRedisContainer(t *testing.T, child bool) (db.GenericDB, func()) {
	client, shutdownFunc := test_utils.StartRedisTestContainer(t)

	//https://redis.io/docs/latest/develop/ai/search-and-query/indexing/#index-limitations:~:text=After%20you%20create,modify%20command%20finishes
	err := client.CreateIndex()
	assert.NoError(t, err, "Expected no error when creating index")

	err = client.SetMetadata("12345", model.Metadata{
		Name:        "collectionName",
		Owner:       "12345",
		CreatedAt:   time.Unix(1745748000, 0),
		Groups:      []string{"groupA", "groupB", "groupC"},
		Consistency: model.EventualConsistent,
		Geolocation: "IT/Como",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 12345")

	err = client.SetMetadata("54321", model.Metadata{
		Name:        "collectionName2",
		Owner:       "12345",
		CreatedAt:   time.Unix(745748000, 0),
		Groups:      []string{"groupC"},
		Consistency: model.EventualConsistent,
		Geolocation: "IT/Milan",
	})
	assert.NoError(t, err, "Expected no error when setting metadata for collection 54321")

	if !child {
		err = client.SetMetadata("1111", model.Metadata{
			Name:        "collectionNameFather",
			Owner:       "12345",
			CreatedAt:   time.Unix(1745749000, 0),
			Groups:      []string{"groupC"},
			Consistency: model.EventualConsistent,
			Geolocation: "IT",
		})
		assert.NoError(t, err, "Expected no error when setting metadata for collection 1111")
	}

	return client, shutdownFunc
}
