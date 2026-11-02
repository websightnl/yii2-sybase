<?php

namespace websightnl\yii2\sybase\conditions;

use yii\base\NotSupportedException;
use yii\db\Expression;

/**
 * InConditionBuilder for SQL Anywhere, which has no row value constructors: `(a, b) IN ((1, 2))`
 * is rewritten as `(a = 1 AND b = 2)`.
 */
class InConditionBuilder extends \yii\db\conditions\InConditionBuilder
{
    /**
     * @inheritdoc
     * @throws NotSupportedException if `$columns` is an array
     */
    protected function buildSubqueryInCondition($operator, $columns, $values, &$params)
    {
        if (is_array($columns)) {
            throw new NotSupportedException(__METHOD__ . ' is not supported by Sybase.');
        }

        return parent::buildSubqueryInCondition($operator, $columns, $values, $params);
    }

    /**
     * @inheritdoc
     */
    protected function buildCompositeInCondition($operator, $columns, $values, &$params)
    {
        $quotedColumns = [];
        foreach ($columns as $i => $column) {
            if ($column instanceof Expression) {
                $column = $column->expression;
            }
            $quotedColumns[$i] = strpos($column, '(') === false ? $this->queryBuilder->db->quoteColumnName($column) : $column;
        }
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($columns as $i => $column) {
                if ($column instanceof Expression) {
                    $column = $column->expression;
                }
                if (isset($value[$column])) {
                    $phName = $this->queryBuilder->bindParam($value[$column], $params);
                    $vs[] = $quotedColumns[$i] . ($operator === 'IN' ? ' = ' : ' != ') . $phName;
                } else {
                    $vs[] = $quotedColumns[$i] . ($operator === 'IN' ? ' IS' : ' IS NOT') . ' NULL';
                }
            }
            $vss[] = '(' . implode($operator === 'IN' ? ' AND ' : ' OR ', $vs) . ')';
        }

        return '(' . implode($operator === 'IN' ? ' OR ' : ' AND ', $vss) . ')';
    }
}
