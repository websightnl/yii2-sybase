# Introduction

This library adds Sybase schema support to yii2. This allows you to create ActiveRecord models of tables; manually or by using Gii.

# Compatibility

It has been tested on SAP SQL Anywhere 12.0 over an ODBC link.

# Usage

Add the following lines to your database configuration (for example config/db.php):

```php
<?php
return [
    'class' => 'yii\db\Connection',
    ***'driverName' => 'sybase',
    'schemaMap' => [
        'sybase' => \websightnl\yii2\sybase\Schema::className(),
    ],***
    'dsn' => 'odbc:mydsn',
]
```