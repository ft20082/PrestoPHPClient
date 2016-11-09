<?php
/**
 * Created by IntelliJ IDEA.
 * User: jake
 * Date: 16/11/9
 * Time: 14:53
 */

namespace kingnetdc;


class PrestoClient {

    private $_source = 'PrestoPHPClient';
    private $_maximumRetries = 5;
    private $_user = 'presto';
    private $_version = '1.0';
    private $_userAgent;
    private $_timezone = '+8';
    private $_language = 'Chinese';

    private $_tempNextUri;
    private $_tempInfoUri;
    private $_tempPartialCancelUri;
    private $_tempState;
    private $_tempResult;
    private $_tempData;

    private $_uri;
    private $_schema;
    private $_catalog;
    private $_headers = [];

    private $_error = [];
    private $_debug = FALSE;
    private $_curlHandle;

    public function __construct($uri, $catalog, $schema, $param = []) {
        $this->_uri = 'http://' . $uri . '/v1/statement';
        $this->_catalog = $catalog;
        $this->_schema = $schema;
        $this->_init($param);
    }

    private function _init($param) {
        $this->_userAgent = $this->_source . '/' . $this->_version;
        $this->_curlHandle = \curl_init();
        $this->_headers = [
            'X-Presto-Catalog:' . $this->_catalog,
            'X-Presto-Source:' . $this->_source,
            'X-Presto-Schema:' . $this->_schema,
            'User-Agent:' . $this->_userAgent,
            'X-Presto-User:' . $this->_user,
            'X-Presto-Time-Zone:' . $this->_timezone,
            'X-Presto-Language:' . $this->_language,
        ];
        if($this->_checkParam($param, 'maximumRetries')) {
            $this->_maximumRetries = (int) $param['maximumRetries'];
        }
        if($this->_checkParam($param, 'user')) {
            $this->_user = $param['user'];
        }
        if($this->_checkParam($param, 'userAgent')) {
            $this->_userAgent = $param['userAgent'];
        }
        if($this->_checkParam($param, 'timezone')) {
            $this->_timezone = $param['timezone'];
        }
        if($this->_checkParam($param, 'language')) {
            $this->_language = $param['language'];
        }
        if($this->_checkParam($param, 'debug')) {
            $this->_debug = (boolean) $param['debug'];
        }
    }

    /**
     * query $sql and get result data
     */
    public function query($sql) {
        $this->_reset();
        $this->_buildQuery($sql);
        return $this->_getQueryData();
    }


    public function getError() {
        return $this->_error;
    }


    /**
     * build query
     */
    private function _buildQuery($sql) {
        $options = [
            CURLOPT_URL => $this->_uri,
            CURLOPT_HTTPHEADER => $this->_headers,
            CURLOPT_RETURNTRANSFER => TRUE,
            CURLOPT_POST => TRUE,
            CURLOPT_POSTFIELDS => $sql,
        ];
        $this->_resetCurlOptions();
        $this->_setCurlOptions($options);
        $this->_tempResult = $this->_getCurlExec();
        $httpCode = $this->_getCurlInfo(CURLINFO_HTTP_CODE);
        if($httpCode != 200) {
            throw new \Exception("Http Curl Error, error Code : " . $httpCode);
        }
    }

    /**
     * sleep for get query result data
     */
    private function _getQueryData() {
        $this->_parseResultData();
        while($this->_tempNextUri) {
            usleep(500000);
            $this->_tempResult = file_get_contents($this->_tempNextUri);
            $this->_parseResultData();
        }
        if($this->_tempState != "FINISHED") {
            throw new \Exception("Presto query Error, error state : " . $this->_tempState);
        }
        return $this->_tempData;
    }

    private function _parseResultData() {
        $retArr = json_decode($this->_tempResult, TRUE);
        if($this->_debug) {
            var_dump('---', $this->_tempResult);
        }
        if(isset($retArr['nextUri'])) {
            $this->_tempNextUri = $retArr['nextUri'];
        } else {
            $this->_tempNextUri = NULL;
        }
        if(isset($retArr['data'])) {
            $this->_tempData = array_merge($this->_tempData, $retArr['data']);
        }
        if(isset($retArr['infoUri'])) {
            $this->_tempInfoUri = $retArr['infoUri'];
        }
        if(isset($retArr['partialCancelUri'])) {
            $this->_tempPartialCancelUri = $retArr['partialCancelUri'];
        }
        if(isset($retArr['stats'])) {
            $this->_tempState = $retArr['stats']['state'];
        }
        if(isset($retArr['error'])) {
            $this->_error = $retArr['error'];
        }
    }

    /**
     * reset temp variable
     */
    private function _reset() {
        $this->_tempNextUri = NULL;
        $this->_tempInfoUri = NULL;
        $this->_tempPartialCancelUri = NULL;
        $this->_tempState = 'NONE';
        $this->_tempResult = NULL;
        $this->_tempData = [];
    }

    private function _getCurlInfo($option = 0) {
        if($option != 0) {
            return \curl_getinfo($this->_curlHandle, $option);
        } else {
            return \curl_getinfo($this->_curlHandle);
        }

    }

    private function _getCurlExec() {
        return \curl_exec($this->_curlHandle);
    }

    private function _setCurlOptions($param) {
        \curl_setopt_array($this->_curlHandle, $param);
    }

    private function _resetCurlOptions() {
        \curl_reset($this->_curlHandle);
    }

    private function _checkParam($param, $key) {
        return isset($param[$key]) && !empty($param[$key]);
    }
}