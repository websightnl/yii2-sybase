<?php

namespace websightnl\yii2\sybase\conditions;

/**
 * LikeConditionBuilder for SQL Anywhere. SQL Anywhere has no default LIKE escape character,
 * so wildcards are escaped with character classes instead of a backslash.
 */
class LikeConditionBuilder extends \yii\db\conditions\LikeConditionBuilder
{
    /**
     * @inheritdoc
     */
    protected $escapingReplacements = [
        '%' => '[%]',
        '_' => '[_]',
        '[' => '[[]',
        ']' => '[]]',
        '\\' => '[\\]',
    ];
}
