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

use kingnetdc\PrestoClient;

require __DIR__ . '/../src/PrestoClient.php';

$uri = 'host:port';
$catalog = 'hive';
$schema = 'default';
$param = [
    //'user' => 'presto',
    //'userAgent' => 'PrestoPHPClient/1.0',
    //'timezone' => '+8',
    //'language' => 'Chinese',
    //'debug' => FALSE,
];

$presto = new PrestoClient($uri, $catalog, $schema, $param);

$sqlArr = [
    "select count(*) from event_00001",
    "select count(*) from event_00001 where ds = '2016-10-14'",
    "select ds, count(*) from event_00001 group by ds",
    "select ds, count(*) from event_00001 where ds > '2016-10-10' group by ds",
    "select ds, count(*) from event_00001 where ds < '2016-10-01' group by ds",
];

try {
    foreach($sqlArr as $sql) {
        var_dump($presto->query($sql));
    }
} catch (\Exception $e) {
    var_dump($presto->getError());
    var_dump($e);
}