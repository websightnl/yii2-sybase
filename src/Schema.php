<?php

namespace websightnl\yii2\sybase\;

use yii\base\InvalidParamException;
use yii\db\ColumnSchema;
use yii\db\TableSchema;

/**
 * Schema is the class for retrieving metadata from a Sybase SQL Anywhere 12 databases.
 *
 * @author Eric Bus <eric.bus@websight.nl>
 * @since 2.0
 */
class Schema extends \yii\db\Schema
{
    const TABLE_TYPE_BASE = 1;
    const TABLE_TYPE_MATERIALIZED_VIEW = 2;
    const TABLE_TYPE_VIEW = 21;

    /**
     * @var string the default schema used for the current session.
     */
    public $defaultSchema = '';
    /**
     * @var array mapping from physical column types (keys) to abstract column types (values)
     */
    public $typeMap = [
        // exact numbers
        'bigint' => self::TYPE_BIGINT,
        'numeric' => self::TYPE_DECIMAL,
        'bit' => self::TYPE_SMALLINT,
        'smallint' => self::TYPE_SMALLINT,
        'decimal' => self::TYPE_DECIMAL,
        'integer' => self::TYPE_INTEGER,
        'tinyint' => self::TYPE_SMALLINT,
        // approximate numbers
        'float' => self::TYPE_FLOAT,
        'double' => self::TYPE_DOUBLE,
        // date and time
        'date' => self::TYPE_DATE,
        'time' => self::TYPE_TIME,
        // character strings
        'char' => self::TYPE_CHAR,
        'varchar' => self::TYPE_STRING,
        'text' => self::TYPE_TEXT,
        // unicode character strings
        'nchar' => self::TYPE_CHAR,
        'nvarchar' => self::TYPE_STRING,
        // binary strings
        'binary' => self::TYPE_BINARY,
        'varbinary' => self::TYPE_BINARY,
        // other data types
        // 'cursor' type cannot be used with tables
        'timestamp' => self::TYPE_TIMESTAMP,
        'uniqueidentifier' => self::TYPE_STRING,
    ];


    /**
     * @inheritdoc
     */
    public function createSavepoint($name)
    {
        $this->db->createCommand("SAVE TRANSACTION $name")->execute();
    }

    /**
     * @inheritdoc
     */
    public function releaseSavepoint($name)
    {
        $this->db->createCommand("RELEASE TRANSACTION $name")->execute();
    }

    /**
     * @inheritdoc
     */
    public function rollBackSavepoint($name)
    {
        $this->db->createCommand("ROLLBACK TRANSACTION $name")->execute();
    }

    /**
     * Quotes a table name for use in a query.
     * A simple table name has no schema prefix.
     * @param string $name table name.
     * @return string the properly quoted table name.
     */
    public function quoteSimpleTableName($name)
    {
        return strpos($name, '"') === false ? "\"{$name}\"" : $name;
    }

    /**
     * Quotes a column name for use in a query.
     * A simple column name has no prefix.
     * @param string $name column name.
     * @return string the properly quoted column name.
     */
    public function quoteSimpleColumnName($name)
    {
        return strpos($name, '"') === false && $name !== '*' ? "\"{$name}\"" : $name;
    }

    /**
     * Creates a query builder for the MSSQL database.
     * @return QueryBuilder query builder interface.
     */
    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    /**
     * Loads the metadata for the specified table.
     * @param string $name table name
     * @return TableSchema|null driver dependent table metadata. Null if the table does not exist.
     */
    public function loadTableSchema($name)
    {
        $table = new TableSchema();
        $this->resolveTableNames($table, $name);
        $this->findPrimaryKeys($table);
        if ($this->findColumns($table)) {
            $this->findForeignKeys($table);

            return $table;
        } else {
            return null;
        }
    }

    /**
     * Resolves the table name and schema name (if any).
     * @param TableSchema $table the table metadata object
     * @param string $name the table name
     */
    protected function resolveTableNames($table, $name)
    {
        $parts = explode('.', str_replace('"', '', $name));
        $partCount = count($parts);
        if ($partCount === 2) {
            // only schema name and table name passed
            $table->schemaName = $parts[0];
            $table->name = $parts[1];
            $table->fullName = $table->schemaName !== $this->defaultSchema ? $table->schemaName . '.' . $table->name : $table->name;
        } else {
            // only table name passed
            $table->schemaName = $this->defaultSchema;
            $table->fullName = $table->name = $parts[0];
        }
    }

    /**
     * Loads the column information into a [[ColumnSchema]] object.
     * @param array $info column information
     * @return ColumnSchema the column schema object
     */
    protected function loadColumnSchema($info)
    {
        $column = $this->createColumnSchema();

        $column->name = $info['column_name'];
        $column->allowNull = $info['nulls'] === 'Y';
        $column->dbType = $info['data_type'];
        $column->enumValues = []; // sybase has only vague equivalents to enum
        $column->isPrimaryKey = null; // primary key will be determined in findColumns() method
        $column->autoIncrement = $info['is_identity'] == 1;
        $column->unsigned = stripos($column->dbType, 'unsigned') !== false;
        $column->comment = $info['comment'] === null ? '' : $info['comment'];

        $column->type = self::TYPE_STRING;
        if (preg_match('/^(\w+)(?:\(([^\)]+)\))?/', $column->dbType, $matches)) {
            $type = $matches[1];
            if (isset($this->typeMap[$type])) {
                $column->type = $this->typeMap[$type];
            }
            if (!empty($matches[2])) {
                $values = explode(',', $matches[2]);
                $column->size = $column->precision = (int) $values[0];
                if (isset($values[1])) {
                    $column->scale = (int) $values[1];
                }
                if ($column->size === 1 && ($type === 'tinyint' || $type === 'bit')) {
                    $column->type = 'boolean';
                } elseif ($type === 'bit') {
                    if ($column->size > 32) {
                        $column->type = 'bigint';
                    } elseif ($column->size === 32) {
                        $column->type = 'integer';
                    }
                }
            }
        }

        $column->phpType = $this->getColumnPhpType($column);

        if ($info['column_default'] === '(NULL)') {
            $info['column_default'] = null;
        }
        if (!$column->isPrimaryKey && ($column->type !== 'timestamp' || $info['column_default'] !== 'CURRENT_TIMESTAMP')) {
            $column->defaultValue = $column->phpTypecast($info['column_default']);
        }

        return $column;
    }

    /**
     * Collects the metadata of table columns.
     * @param TableSchema $table the table metadata
     * @return boolean whether the table exists in the database
     */
    protected function findColumns($table)
    {

        $columnsTableName = 'SYSTABCOL';
        $tablesTableName = 'SYSTAB';
        $objectsTableName = 'SYSOBJECT';
        $remarksTableName = 'SYSREMARK';
        $domainsTableName = 'SYSDOMAIN';
        $whereSql = "t2.table_name = '{$table->name}'";

        $columnsTableName = $this->quoteTableName($columnsTableName);
        $tablesTableName = $this->quoteTableName($tablesTableName);
        $objectsTableName = $this->quoteTableName($objectsTableName);
        $remarksTableName = $this->quoteTableName($remarksTableName);
        $domainsTableName = $this->quoteTableName($domainsTableName);

        $sql = <<<SQL
SELECT t1.column_name, t1.nulls, STRING(t5.domain_name, '(', t1.width, ')') AS data_type, t1."default" AS column_default, (IF t1."default" = 'autoincrement' THEN 1 ELSE 0 END IF) AS is_identity, 
t4.remarks AS "comment"
FROM {$columnsTableName} AS t1
INNER JOIN {$tablesTableName} AS t2 
ON t2.table_id = t1.table_id 
INNER JOIN {$objectsTableName} AS t3 
ON t3.object_id = t1.object_id 
LEFT JOIN {$remarksTableName} AS t4
ON t4.object_id = t1.object_id
INNER JOIN {$domainsTableName} AS t5 
ON t5.domain_id = t1.domain_id 
WHERE {$whereSql}
SQL;

        try {
            $columns = $this->db->createCommand($sql)->queryAll();
            if (empty($columns)) {
                return false;
            }
        } catch (\Exception $e) {
            return false;
        }
        foreach ($columns as $column) {
            $column = $this->loadColumnSchema($column);
            foreach ($table->primaryKey as $primaryKey) {
                if (strcasecmp($column->name, $primaryKey) === 0) {
                    $column->isPrimaryKey = true;
                    break;
                }
            }
            if ($column->isPrimaryKey && $column->autoIncrement) {
                $table->sequenceName = '';
            }
            $table->columns[$column->name] = $column;
        }

        return true;
    }

    /**
     * Collects the constraint details for the given table and constraint type.
     * @param TableSchema $table
     * @param string $type either PRIMARY KEY or UNIQUE
     * @return array each entry contains index_name and field_name
     * @since 2.0.4
     */
    protected function findTableConstraints($table, $type)
    {
        switch ($type) {
            case 'PRIMARY KEY': $indexCategory = 1; break;
            case 'UNIQUE': $indexCategory = 3; break;
            default:
                throw new InvalidParamException(\Yii::t('app', 'Invalid type parameter, use PRIMARY KEY or UNIQUE'));
        }

        $sql = <<<SQL
SELECT SYSIDX.index_name, SYSCOLUMN.column_name AS field_name
FROM SYSIDX
INNER JOIN SYSTABLE
    ON SYSTABLE.table_id = SYSIDX.table_id
    AND SYSTABLE.table_name = :tableName
INNER JOIN SYSIDXCOL
    ON SYSIDXCOL.index_id = SYSIDX.index_id
    AND SYSIDXCOL.table_id = SYSIDX.table_id
INNER JOIN SYSCOLUMN
    ON SYSIDXCOL.column_id = SYSCOLUMN.column_id
    AND SYSIDXCOL.table_id = SYSCOLUMN.table_id
WHERE SYSIDX.index_category = :indexCategory
SQL;

        return $this->db
            ->createCommand($sql, [
                ':tableName' => $table->name,
                ':indexCategory' => $indexCategory
            ])
            ->queryAll();
    }

    /**
     * Collects the primary key column details for the given table.
     * @param TableSchema $table the table metadata
     */
    protected function findPrimaryKeys($table)
    {
        $result = [];
        foreach ($this->findTableConstraints($table, 'PRIMARY KEY') as $row) {
            $result[] = $row['field_name'];
        }
        $table->primaryKey = $result;
    }

    /**
     * Collects the foreign key column details for the given table.
     * @param TableSchema $table the table metadata
     */
    protected function findForeignKeys($table)
    {
        $fkeyTableName = 'SYSFKEY';
        $tablesTableName = 'SYSTAB';
        $columnsTableName = 'SYSTABCOL';
        $indexTableName = 'SYSIDXCOL';

        $fkeyTableName = $this->quoteTableName($fkeyTableName);
        $tablesTableName = $this->quoteTableName($tablesTableName);
        $columnsTableName = $this->quoteTableName($columnsTableName);

        $sql = <<<SQL
SELECT pt4.column_name AS "fk_column_name", ft2.table_name AS "uq_table_name", ft4.column_name AS "uq_column_name"
FROM {$fkeyTableName} AS t1
INNER JOIN {$tablesTableName} AS pt2
ON pt2.table_id = t1.primary_table_id
INNER JOIN {$indexTableName} AS pt3
ON pt3.table_id = t1.primary_table_id
AND pt3.index_id = t1.primary_index_id
INNER JOIN {$columnsTableName} AS pt4
ON pt4.table_id = t1.primary_table_id
AND pt4.column_id = pt3.column_id
INNER JOIN {$tablesTableName} AS ft2
ON ft2.table_id = t1.foreign_table_id
INNER JOIN {$indexTableName} AS ft3
ON ft3.table_id = t1.foreign_table_id
AND ft3.index_id = t1.foreign_index_id
INNER JOIN {$columnsTableName} AS ft4
ON ft4.table_id = t1.foreign_table_id
AND ft4.column_id = pt3.column_id
WHERE pt2.table_name = :tableName
SQL;

        $rows = $this->db->createCommand($sql, [
            ':tableName' => $table->name
        ])->queryAll();
        $table->foreignKeys = [];
        foreach ($rows as $row) {
            $table->foreignKeys[] = [$row['uq_table_name'], $row['fk_column_name'] => $row['uq_column_name']];
        }
    }

    /**
     * Returns all table names in the database.
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     * @return array all table names in the database. The names have NO schema name prefix.
     */
    protected function findTableNames($schema = '')
    {
        $tablesTableName = 'SYSTAB';
        $tableTypes = join(',', [self::TABLE_TYPE_BASE, self::TABLE_TYPE_VIEW]);

        $sql = <<<SQL
SELECT t.table_name
FROM {$tablesTableName} AS t
WHERE t.table_type IN ({$tableTypes})
ORDER BY t.table_name
SQL;

        return $this->db->createCommand($sql)->queryColumn();
    }

    /**
     * Returns all unique indexes for the given table.
     * Each array element is of the following structure:
     *
     * ```php
     * [
     *     'IndexName1' => ['col1' [, ...]],
     *     'IndexName2' => ['col2' [, ...]],
     * ]
     * ```
     *
     * @param TableSchema $table the table metadata
     * @return array all unique indexes for the given table.
     * @since 2.0.4
     */
    public function findUniqueIndexes($table)
    {
        $result = [];
        foreach ($this->findTableConstraints($table, 'UNIQUE') as $row) {
            $result[$row['index_name']][] = $row['field_name'];
        }
        return $result;
    }
}
