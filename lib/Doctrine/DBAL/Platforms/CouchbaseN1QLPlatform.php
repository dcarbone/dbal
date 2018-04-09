<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\DBAL\Platforms;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\Schema\Table;

/**
 * The CouchbaseN1QLPlatform class describes the specifics of using N1QL to interact with the
 * Couchbase database platform.
 *
 * @author Daniel Carbone <daniel.p.carbone@gmail.com>
 */
class CouchbaseN1QLPlatform extends AbstractPlatform
{
    const PLATFORM_NAME = 'couchbase-n1ql';

    /**
     * @inheritDoc
     */
    public function getIdentifierQuoteCharacter()
    {
        return '`';
    }

    /**
     * @inheritDoc
     */
    public function getSqlCommentStartString()
    {
        return '/* ';
    }

    /**
     * @inheritDoc
     */
    public function getSqlCommentEndString()
    {
        return ' */';
    }

    /**
     * @inheritDoc
     */
    public function getRegexpExpression($expr, $pattern)
    {
        return "REGEXP_CONTAINS({$expr}, {$pattern})";
    }

    /**
     * @inheritDoc
     */
    public function getGuidExpression()
    {
        return 'UUID()';
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getMd5Expression($column)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getModExpression($expression1, $expression2)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     */
    public function getTrimExpression($str, $mode = TrimMode::UNSPECIFIED, $char = false)
    {
        $trimChar = (false !== $char) ? (', ' . $char) : '';

        switch ($mode) {
            case TrimMode::LEADING:
                return "LTRIM({$str}, {$trimChar})";
            case TrimMode::TRAILING:
                return "RTRIM({$str}, {$trimChar})";

            default:
                return "TRIM({$str}, {$trimChar})";
        }
    }

    /**
     * NOTE: does not support a start offset other than 0.
     *
     * @inheritDoc
     */
    public function getLocateExpression($str, $substr, $startPos = false)
    {
        if (false !== $startPos) {
            throw DBALException::notSupported(sprintf('%s with $startPos defined', __METHOD__));
        }
        return "POSITION({$str}, {$substr})";
    }

    /**
     * @inheritDoc
     *
     * @param string $type
     */
    public function getNowExpression(?string $type = 'timestamp')
    {
        switch ($type) {
            case 'timestamp':
                return 'NOW_TZ()';
            case 'local':
                return 'NOW_LOCAL()';
            case 'millis':
                return 'NOW_MILLIS()';

            default;
                return 'NOW_UTC()';
        }
    }

    /**
     * @inheritDoc
     */
    public function getSubstringExpression($value, $from, $length = null)
    {
        if (null === $length) {
            return "SUBSTR({$value}, {$from})";
        }
        return "SUBSTR({$value}, {$from}, {$length})";
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getNotExpression($expression)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     */
    public function getDateDiffExpression($date1, $date2)
    {
        // TODO: need to differentiate between millis and string dates, or just pick one.

        parent::getDateDiffExpression($date1, $date2);
    }

    /**
     * @inheritDoc
     */
    public function getBitAndComparisonExpression($value1, $value2)
    {
        return "BITAND({$value1}, {$value2})";
    }

    /**
     * @inheritDoc
     */
    public function getBitOrComparisonExpression($value1, $value2)
    {
        return "BITOR({$value1}, {$value2})";
    }

    /**
     * @inheritDoc
     */
    protected function getDateArithmeticIntervalExpression($date, $operator, $interval, $unit)
    {
        if (is_int($date) || (is_string($date) && ctype_digit($date))) {
            $op = "DATE_ADD_MILLIS({$date}";
        } else {
            $op = "DATE_ADD_STR(\"{$date}\"";
        }

        if ('-' === $operator) {
            $op .= ",-{$interval}";
        } else {
            $op .= ",{$interval}";
        }

        return ",{$op}" . strtolower($unit) . ')';
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getForUpdateSQL()
    {
        // TODO: Probably possible.
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function appendLockHint($fromClause, $lockMode)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getReadLockSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getWriteLockSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getDropDatabaseSQL($database)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getDropTableSQL($table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getDropTemporaryTableSQL($table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws InvalidArgumentException
     */
    public function getDropIndexSQL($index, $table = null, $useGSI = false)
    {
        if ('' === (string)$table) {
            throw new InvalidArgumentException('$table argument is required with ' . __CLASS__);
        }
        $sql = "DROP INDEX {$table}.{$index}";
        if ($useGSI) {
            return "{$sql} USING GSI";
        }
        return $sql;
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getDropConstraintSQL($constraint, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getDropForeignKeySQL($foreignKey, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableSQL(Table $table, $createFlags = self::CREATE_INDEXES)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     * @throws DBALException
     */
    public function getCommentOnColumnSQL($tableName, $columnName, $comment)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * @inheritDoc
     */
    public function supportsAlterTable()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsTransactions()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsSavepoints()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsReleaseSavepoints()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsPrimaryConstraints()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsForeignKeyConstraints()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsForeignKeyOnUpdate()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsInlineColumnComments()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsCreateDropDatabase()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function supportsGettingAffectedRows()
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function getDateTimeFormatString()
    {
        return \Datetime::ISO8601;
    }

    /**
     * @inheritDoc
     */
    public function getDateTimeTzFormatString()
    {
        return \DateTime::ISO8601;
    }

    /**
     * @inheritDoc
     */
    public function getDateFormatString()
    {
        // TODO: Will this work?
        return \DateTime::ISO8601;
    }

    /**
     * @inheritDoc
     */
    public function getTimeFormatString()
    {
        // TODO: Will this work?
        return \DateTime::ISO8601;
    }

    /**
     * @param $expression
     * @return string
     */
    public function getIsMissingExpression($expression)
    {
        return "{$expression} IS MISSING";
    }

    /**
     * @param $expression
     * @return string
     */
    public function getIsNotMissingExpression($expression)
    {
        return "{$expression} IS NOT MISSING";
    }

    /**
     * @inheritDoc
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    protected function initializeDoctrineTypeMappings()
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getClobTypeDeclarationSQL(array $field)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        return '';
    }

    /**
     * @inheritDoc
     */
    public function getName()
    {
        return self::PLATFORM_NAME;
    }
}