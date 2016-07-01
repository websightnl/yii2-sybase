<?php

namespace websight\yii2\sybase;

use yii\base\InvalidParamException;
use yii\base\NotSupportedException;

/**
 * QueryBuilder is the query builder for MS SQL Server databases (version 2008 and above).
 *
 * @author Timur Ruziev <resurtm@gmail.com>
 * @since 2.0
 */
class QueryBuilder extends \yii\db\QueryBuilder
{
    /**
     * @var array mapping from abstract column types (keys) to physical column types (values).
     */
    public $typeMap = [
        Schema::TYPE_PK => 'integer IDENTITY PRIMARY KEY',
        Schema::TYPE_UPK => 'integer IDENTITY PRIMARY KEY',
        Schema::TYPE_BIGPK => 'bigint IDENTITY PRIMARY KEY',
        Schema::TYPE_UBIGPK => 'bigint IDENTITY PRIMARY KEY',
        Schema::TYPE_CHAR => 'nchar(1)',
        Schema::TYPE_STRING => 'nvarchar(255)',
        Schema::TYPE_TEXT => 'ntext',
        Schema::TYPE_SMALLINT => 'smallint',
        Schema::TYPE_INTEGER => 'integer',
        Schema::TYPE_BIGINT => 'bigint',
        Schema::TYPE_FLOAT => 'float',
        Schema::TYPE_DOUBLE => 'float',
        Schema::TYPE_DECIMAL => 'decimal',
        Schema::TYPE_DATETIME => 'datetime',
        Schema::TYPE_TIMESTAMP => 'timestamp',
        Schema::TYPE_TIME => 'time',
        Schema::TYPE_DATE => 'date',
        Schema::TYPE_BINARY => 'binary(1)',
        Schema::TYPE_BOOLEAN => 'bit',
        Schema::TYPE_MONEY => 'decimal(19,4)',
    ];


    /**
     * @inheritdoc
     */
    public function buildOrderByAndLimit($sql, $orderBy, $limit, $offset)
    {
        if (!$this->hasOffset($offset) && !$this->hasLimit($limit)) {
            $orderBy = $this->buildOrderBy($orderBy);
            return $orderBy === '' ? $sql : $sql . $this->separator . $orderBy;
        }

        $orderBy = $this->buildOrderBy($orderBy);
        if ($orderBy === '') {
            // ROW_NUMBER() requires an ORDER BY clause
            $orderBy = 'ORDER BY (SELECT NULL)';
        }

        if ($this->hasLimit($limit)) {
            $orderAndLimitSql = "TOP $limit";
        } else {
            $orderAndLimitSql = '';
        }
        if ($this->hasOffset($offset)) {
            $orderAndLimitSql .= $this->separator . "START AT $offset";
        }

        $sql = preg_replace('/^([\s(])*SELECT(\s+DISTINCT)?(?!\s*TOP\s*\()/i', "\\1SELECT\\2 $orderAndLimitSql", $sql);
        $sql .= $this->separator . $orderBy;

        return $sql;
    }

    /**
     * Builds a SQL statement for renaming a DB table.
     * @param string $oldName the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     * @return string the SQL statement for renaming a DB table.
     */
    public function renameTable($oldName, $newName)
    {
        return 'sp_rename ' . $this->db->quoteTableName($oldName) . ', ' . $this->db->quoteTableName($newName);
    }

    /**
     * Builds a SQL statement for renaming a column.
     * @param string $table the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $oldName the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn($table, $oldName, $newName)
    {
        $table = $this->db->quoteTableName($table);
        $oldName = $this->db->quoteColumnName($oldName);
        $newName = $this->db->quoteColumnName($newName);
        return "sp_rename '{$table}.{$oldName}', {$newName}, 'COLUMN'";
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     * @param string $table the table whose column is to be changed. The table name will be properly quoted by the method.
     * @param string $column the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $type the new column type. The [[getColumnType]] method will be invoked to convert abstract column type (if any)
     * into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     * For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     * @return string the SQL statement for changing the definition of a column.
     */
    public function alterColumn($table, $column, $type)
    {
        $type = $this->getColumnType($type);
        $sql = 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ALTER COLUMN '
            . $this->db->quoteColumnName($column) . ' '
            . $this->getColumnType($type);

        return $sql;
    }

    /**
     * Builds a SQL statement for enabling or disabling integrity check.
     * @param boolean $check whether to turn on or off the integrity check.
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     * @param string $table the table name. Defaults to empty string, meaning that no table will be changed.
     * @return string the SQL statement for checking integrity
     * @throws InvalidParamException if the table does not exist or there is no sequence associated with the table.
     */
    public function checkIntegrity($check = true, $schema = '', $table = '')
    {
        if ($schema !== '') {
            $table = "{$schema}.{$table}";
        }
        $table = $this->db->quoteTableName($table);
        if ($this->db->getTableSchema($table) === null) {
            throw new InvalidParamException("Table not found: $table");
        }
        $enable = $check ? 'CHECK' : 'NOCHECK';

        return "ALTER TABLE {$table} {$enable} CONSTRAINT ALL";
    }

    /**
     * @inheritdoc
     * @since 2.0.8
     */
    public function addCommentOnColumn($table, $column, $comment)
    {
        return "sp_updateextendedproperty @name = N'MS_Description', @value = {$this->db->quoteValue($comment)}, @level1type = N'Table',  @level1name = {$this->db->quoteTableName($table)}, @level2type = N'Column', @level2name = {$this->db->quoteColumnName($column)}";
    }

    /**
     * @inheritdoc
     * @since 2.0.8
     */
    public function addCommentOnTable($table, $comment)
    {
        return "sp_updateextendedproperty @name = N'MS_Description', @value = {$this->db->quoteValue($comment)}, @level1type = N'Table',  @level1name = {$this->db->quoteTableName($table)}";
    }

    /**
     * @inheritdoc
     * @since 2.0.8
     */
    public function dropCommentFromColumn($table, $column)
    {
        return "sp_dropextendedproperty @name = N'MS_Description', @level1type = N'Table',  @level1name = {$this->db->quoteTableName($table)}, @level2type = N'Column', @level2name = {$this->db->quoteColumnName($column)}";
    }

    /**
     * @inheritdoc
     * @since 2.0.8
     */
    public function dropCommentFromTable($table)
    {
        return "sp_dropextendedproperty @name = N'MS_Description', @level1type = N'Table',  @level1name = {$this->db->quoteTableName($table)}";
    }

    /**
     * Returns an array of column names given model name
     *
     * @param string $modelClass name of the model class
     * @return array|null array of column names
     */
    protected function getAllColumnNames($modelClass = null)
    {
        if (!$modelClass) {
            return null;
        }
        /* @var $model \yii\db\ActiveRecord */
        $model = new $modelClass;
        $schema = $model->getTableSchema();
        $columns = array_keys($schema->columns);
        return $columns;
    }

    /**
     * @inheritdoc
     * @throws NotSupportedException if `$columns` is an array
     */
    protected function buildSubqueryInCondition($operator, $columns, $values, &$params)
    {
        if (is_array($columns)) {
            throw new NotSupportedException(__METHOD__ . ' is not supported by MSSQL.');
        }
        return parent::buildSubqueryInCondition($operator, $columns, $values, $params);
    }

    /**
     * Builds SQL for IN condition
     *
     * @param string $operator
     * @param array $columns
     * @param array $values
     * @param array $params
     * @return string SQL
     */
    protected function buildCompositeInCondition($operator, $columns, $values, &$params)
    {
        $quotedColumns = [];
        foreach ($columns as $i => $column) {
            $quotedColumns[$i] = strpos($column, '(') === false ? $this->db->quoteColumnName($column) : $column;
        }
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($columns as $i => $column) {
                if (isset($value[$column])) {
                    $phName = self::PARAM_PREFIX . count($params);
                    $params[$phName] = $value[$column];
                    $vs[] = $quotedColumns[$i] . ($operator === 'IN' ? ' = ' : ' != ') . $phName;
                } else {
                    $vs[] = $quotedColumns[$i] . ($operator === 'IN' ? ' IS' : ' IS NOT') . ' NULL';
                }
            }
            $vss[] = '(' . implode($operator === 'IN' ? ' AND ' : ' OR ', $vs) . ')';
        }

        return '(' . implode($operator === 'IN' ? ' OR ' : ' AND ', $vss) . ')';
    }

    /**
     * @inheritdoc
     * @since 2.0.8
     */
    public function selectExists($rawSql)
    {
        return 'SELECT CASE WHEN EXISTS(' . $rawSql . ') THEN 1 ELSE 0 END';
    }
}
