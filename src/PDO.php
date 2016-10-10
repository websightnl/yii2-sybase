<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace websightnl\yii2\sybase;

/**
 * This is an extension of the default PDO class of Sybase drivers.
 * It provides workarounds for improperly implemented functionalities of the Sybase drivers.
 *
 * @author Timur Ruziev <resurtm@gmail.com>
 * @since 2.0
 */
class PDO extends \PDO
{
    /**
     * Returns value of the last inserted ID.
     * @param string|null $sequence the sequence name. Defaults to null.
     * @return integer last inserted ID value.
     */
    public function lastInsertId($sequence = null)
    {
        return $this->query('SELECT @@IDENTITY')->fetchColumn();
    }

    /**
     * Retrieve a database connection attribute.
     * It is necessary to override PDO's method as some MSSQL PDO driver (e.g. dblib) does not
     * support getting attributes
     * @param integer $attribute One of the PDO::ATTR_* constants.
     * @return mixed A successful call returns the value of the requested PDO attribute.
     * An unsuccessful call returns null.
     */
    public function getAttribute($attribute)
    {
        try {
            return parent::getAttribute($attribute);
        } catch (\PDOException $e) {
            switch ($attribute) {
                case PDO::ATTR_SERVER_VERSION:
                    return $this->query("SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR)")->fetchColumn();
                default:
                    throw $e;
            }
        }
    }
}
