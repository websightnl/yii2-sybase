# Introduction

This library adds Sybase schema support to yii2. This allows you to create ActiveRecord models of tables; manually or by using Gii.

# Compatibility

Version 1.x requires PHP 8.0+ and Yii 2.0.14+. Use 0.1.x for older PHP/Yii versions.

It has been tested on SAP SQL Anywhere 12.0 over an ODBC link (pdo_odbc), using the SQL Anywhere 12 and 17 client drivers.

# Usage

Add the following lines to your database configuration (for example config/db.php):

```php
<?php
return [
    'class' => 'yii\db\Connection',
    'driverName' => 'sybase',
    'pdoClass' => \websightnl\yii2\sybase\PDO::class,
    'schemaMap' => [
        'sybase' => \websightnl\yii2\sybase\Schema::class,
    ],
    'dsn' => 'odbc:mydsn',
];
```

A DSN-less connection string works as well, for example:

```php
'dsn' => 'odbc:Driver={SQL Anywhere 17};Host=127.0.0.1:2638;ServerName=myserver;DatabaseName=mydb',
```

# Changes in 1.0

* PHP 8 compatible `PDO` signatures.
* `IN` and `LIKE` conditions use Yii 2.0.14+ condition builders. Composite `IN` is rewritten to `AND`/`OR` and
  `LIKE` wildcards are escaped with character classes (`[%]`), as SQL Anywhere has no default escape character.
* `OFFSET` now maps to `START AT offset + 1` (SQL Anywhere is 1-based); previously one row was repeated per page.
* Table names in schema lookups are bound as parameters.
