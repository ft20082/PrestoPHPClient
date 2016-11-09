<?php
/**
 * Created by IntelliJ IDEA.
 * User: jake
 * Date: 16/11/9
 * Time: 17:33
 */

namespace kingnetdc;

use kingnetdc\PrestoClient;

require __DIR__ . '/../src/PrestoClient.php';

$uri = '192.168.78.47:8688';
$catalog = 'hive';
$schema = 'zuotq';
$param = [
    //'debug' => TRUE,
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