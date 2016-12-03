<?php
/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

namespace kingnetdc;



class PrestoClient {

    private $_source = 'PrestoPHPClient';
    private $_user = 'presto';
    private $_version = '1.0';
    private $_userAgent;
    private $_timezone = '+8';
    private $_language = 'Chinese';

    private $_tempNextUri = NULL;
    private $_tempInfoUri;
    private $_tempPartialCancelUri;
    private $_tempState;
    private $_tempResult;
    private $_tempColumns;
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
        $this->_curlHandle = new SimpleCurl();
        $this->_headers = array(
            'X-Presto-Catalog:' . $this->_catalog,
            'X-Presto-Source:' . $this->_source,
            'X-Presto-Schema:' . $this->_schema,
            'User-Agent:' . $this->_userAgent,
            'X-Presto-User:' . $this->_user,
            'X-Presto-Time-Zone:' . $this->_timezone,
            'X-Presto-Language:' . $this->_language,
        );
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
     * @param $sql
     * @return mixed
     */
    public function query($sql) {
        $this->_reset();
        $this->_buildQuery($sql);
        return $this->_getQueryData();
    }

    /**
     * get columns
     * @return mixed
     */
    public function getColumns() {
        return $this->_tempColumns;
    }

    /**
     * get error info
     * @return array
     */
    public function getError() {
        return $this->_error;
    }


    /**
     * @param $sql
     * @throws \Exception
     */
    private function _buildQuery($sql) {
        $this->_curlHandle->reset();
        $this->_curlHandle->setHeader($this->_headers);
        $this->_tempResult = $this->_curlHandle->getPostContent($this->_uri, $sql);
        $httpCode = $this->_curlHandle->getHttpCode();
        if($httpCode != 200) {
            throw new \Exception("Http Curl Error, error Code : " . $httpCode);
        }
    }

    /**
     * sleep for get query result data
     * check http response code 200 or 503
     */
    private function _getQueryData() {
        $this->_parseResultData();
        $this->_curlHandle->reset();
        while($this->_tempNextUri) {
            $this->_tempResult = $this->_curlHandle->getContent($this->_tempNextUri);
            $httpCode = $this->_curlHandle->getHttpCode();
            if($httpCode == 200) {
                $this->_parseResultData();
            } else if($httpCode != 503) {
                $this->_error[] = $this->_curlHandle->getError();
                throw new \Exception("Get query http response code error.");
            }
            usleep(200000);
        }
        if($this->_tempState != "FINISHED") {
            throw new \Exception("Presto query Error, error state : " . $this->_tempState);
        }
        return $this->_tempData;
    }

    /**
     * parse result data every time
     */
    private function _parseResultData() {
        $retArr = json_decode($this->_tempResult, TRUE);
        if($this->_debug) {
            var_dump('debug: ', $this->_tempResult);
        }
        if($retArr) {
            if(isset($retArr['nextUri'])) {
                $this->_tempNextUri = $retArr['nextUri'];
            } else {
                $this->_tempNextUri = NULL;
            }
            if(isset($retArr['data'])) {
                $this->_tempData = array_merge($this->_tempData, $retArr['data']);
            }
            if(isset($retArr['columns'])) {
                $this->_tempColumns = $retArr['columns'];
            }
            if(isset($retArr['infoUri'])) {
                $this->_tempInfoUri = $retArr['infoUri'];
            }
            if(isset($retArr['partialCancelUri'])) {
                $this->_tempPartialCancelUri = $retArr['partialCancelUri'];
            }
            if(isset($retArr['stats']['state'])) {
                $this->_tempState = $retArr['stats']['state'];
            }
            if(isset($retArr['error'])) {
                $this->_error = $retArr['error'];
            }
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
        $this->_tempData = array();
        $this->_tempColumns = array();
    }

    private function _checkParam($param, $key) {
        return isset($param[$key]) && !empty($param[$key]);
    }
}


/**
 * Class SimpleCurl
 * @package kingnetdc
 */
class SimpleCurl {

    private $_handle;
    private $_timeout = 10;
    private $_header = array();

    public function __construct() {
        if(!function_exists("curl_init")) {
            throw new \Exception("Presto Client need PHP Curl module!");
        }
        $this->_handle = \curl_init();
    }

    public function setHeader($header) {
        $this->_header = $header;
    }

    public function setTimeout($timeout) {
        $this->_timeout = $timeout;
    }

    public function getPostContent($url, $postFiled) {
        return $this->getContent($url, "POST", $postFiled);
    }

    public function getContent($url, $method = "GET", $postField = "") {
        \curl_setopt($this->_handle, CURLOPT_URL, $url);
        \curl_setopt($this->_handle, CURLOPT_RETURNTRANSFER, TRUE);
        if(!empty($this->_header)) {
            \curl_setopt($this->_handle, CURLOPT_HTTPHEADER, $this->_header);
        }
        if($method === "POST") {
            \curl_setopt($this->_handle, CURLOPT_POST, TRUE);
            \curl_setopt($this->_handle, CURLOPT_POSTFIELDS, $postField);
        }
        return \curl_exec($this->_handle);
    }

    public function reset() {
        $this->_header = array();
        \curl_reset($this->_handle);
    }

    public function setCurlOptions($options) {
        \curl_setopt_array($this->_handle, $options);
    }

    public function getHttpCode() {
        return $this->getCurlInfo(CURLINFO_HTTP_CODE);
    }

    public function getCurlInfo($option = 0) {
        return \curl_getinfo($this->_handle, $option);
    }

    public function getError() {
        return \curl_error($this->_handle);
    }

    public function __destruct() {
        \curl_close($this->_handle);
    }

}