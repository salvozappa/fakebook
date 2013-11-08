<?php

/*
 * This file is part of the Predis package.
 *
 * (c) Daniele Alessandri <suppakilla@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Predis\Command;

/**
 * Defines an abstraction representing a Redis command.
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandInterface
{
    /**
     * Gets the ID of a Redis command.
     *
     * @return string
     */
    public function getId();

    /**
     * Set the hash for the command.
     *
     * @param int $hash Calculated hash.
     */
    public function setHash($hash);

    /**
     * Returns the hash of the command.
     *
     * @return int
     */
    public function getHash();

    /**
     * Sets the arguments for the command.
     *
     * @param array $arguments List of arguments.
     */
    public function setArguments(Array $arguments);

    /**
     * Sets the raw arguments for the command without processing them.
     *
     * @param array $arguments List of arguments.
     */
    public function setRawArguments(Array $arguments);

    /**
     * Gets the arguments of the command.
     *
     * @return array
     */
    public function getArguments();

    /**
     * Gets the argument of the command at the specified index.
     *
     * @return array
     */
    public function getArgument($index);

    /**
     * Parses a reply buffer and returns a PHP object.
     *
     * @param string $data Binary string containing the whole reply.
     * @return mixed
     */
    public function parseResponse($data);
}

/**
 * Base class for Redis commands.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class AbstractCommand implements CommandInterface
{
    private $hash;
    private $arguments = array();

    /**
     * Returns a filtered array of the arguments.
     *
     * @param array $arguments List of arguments.
     * @return array
     */
    protected function filterArguments(Array $arguments)
    {
        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function setArguments(Array $arguments)
    {
        $this->arguments = $this->filterArguments($arguments);
        unset($this->hash);
    }

    /**
     * Sets the arguments array without filtering.
     *
     * @param array $arguments List of arguments.
     */
    public function setRawArguments(Array $arguments)
    {
        $this->arguments = $arguments;
        unset($this->hash);
    }

    /**
     * {@inheritdoc}
     */
    public function getArguments()
    {
        return $this->arguments;
    }

    /**
     * Gets the argument from the arguments list at the specified index.
     *
     * @param array $arguments Position of the argument.
     */
    public function getArgument($index)
    {
        if (isset($this->arguments[$index])) {
            return $this->arguments[$index];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setHash($hash)
    {
        $this->hash = $hash;
    }

    /**
     * {@inheritdoc}
     */
    public function getHash()
    {
        if (isset($this->hash)) {
            return $this->hash;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return $data;
    }

    /**
     * Helper function used to reduce a list of arguments to a string.
     *
     * @param string $accumulator Temporary string.
     * @param string $argument Current argument.
     * @return string
     */
    protected function toStringArgumentReducer($accumulator, $argument)
    {
        if (strlen($argument) > 32) {
            $argument = substr($argument, 0, 32) . '[...]';
        }

        $accumulator .= " $argument";

        return $accumulator;
    }

    /**
     * Returns a partial string representation of the command with its arguments.
     *
     * @return string
     */
    public function __toString()
    {
        return array_reduce(
            $this->getArguments(),
            array($this, 'toStringArgumentReducer'),
            $this->getId()
        );
    }

    /**
     * Normalizes the arguments array passed to a Redis command.
     *
     * @param array $arguments Arguments for a command.
     * @return array
     */
    public static function normalizeArguments(Array $arguments)
    {
        if (count($arguments) === 1 && is_array($arguments[0])) {
            return $arguments[0];
        }

        return $arguments;
    }

    /**
     * Normalizes the arguments array passed to a variadic Redis command.
     *
     * @param array $arguments Arguments for a command.
     * @return array
     */
    public static function normalizeVariadic(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[1])) {
            return array_merge(array($arguments[0]), $arguments[1]);
        }

        return $arguments;
    }
}

/**
 * Defines a command whose keys can be prefixed.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface PrefixableCommandInterface
{
    /**
     * Prefixes all the keys found in the arguments of the command.
     *
     * @param string $prefix String used to prefix the keys.
     */
    public function prefixKeys($prefix);
}

/**
 * Base class for Redis commands with prefixable keys.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class PrefixableCommand extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        if ($arguments = $this->getArguments()) {
            $arguments[0] = "$prefix{$arguments[0]}";
            $this->setRawArguments($arguments);
        }
    }
}

/**
 * @link http://redis.io/commands/zrange
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRange extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZRANGE';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 4) {
            $lastType = gettype($arguments[3]);

            if ($lastType === 'string' && strtoupper($arguments[3]) === 'WITHSCORES') {
                // Used for compatibility with older versions
                $arguments[3] = array('WITHSCORES' => true);
                $lastType = 'array';
            }

            if ($lastType === 'array') {
                $options = $this->prepareOptions(array_pop($arguments));
                return array_merge($arguments, $options);
            }
        }

        return $arguments;
    }

    /**
     * Returns a list of options and modifiers compatible with Redis.
     *
     * @param array $options List of options.
     * @return array
     */
    protected function prepareOptions($options)
    {
        $opts = array_change_key_case($options, CASE_UPPER);
        $finalizedOpts = array();

        if (!empty($opts['WITHSCORES'])) {
            $finalizedOpts[] = 'WITHSCORES';
        }

        return $finalizedOpts;
    }

    /**
     * Checks for the presence of the WITHSCORES modifier.
     *
     * @return Boolean
     */
    protected function withScores()
    {
        $arguments = $this->getArguments();

        if (count($arguments) < 4) {
            return false;
        }

        return strtoupper($arguments[3]) === 'WITHSCORES';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        if ($this->withScores()) {
            $result = array();

            for ($i = 0; $i < count($data); $i++) {
                $result[] = array($data[$i], $data[++$i]);
            }

            return $result;
        }

        return $data;
    }
}

/**
 * @link http://redis.io/commands/sinter
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetIntersection extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SINTER';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeArguments($arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/sinterstore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetIntersectionStore extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SINTERSTORE';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[1])) {
            return array_merge(array($arguments[0]), $arguments[1]);
        }

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/eval
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerEval extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EVAL';
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        if ($arguments = $this->getArguments()) {
            for ($i = 2; $i < $arguments[1] + 2; $i++) {
                $arguments[$i] = "$prefix{$arguments[$i]}";
            }

            $this->setRawArguments($arguments);
        }
    }

    /**
     * Calculates the SHA1 hash of the body of the script.
     *
     * @return string SHA1 hash.
     */
    public function getScriptHash()
    {
        return sha1($this->getArgument(0));
    }
}

/**
 * @link http://redis.io/commands/blpop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopFirstBlocking extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BLPOP';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[0])) {
            list($arguments, $timeout) = $arguments;
            array_push($arguments, $timeout);
        }

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::skipLast($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/rpush
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPushTail extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RPUSH';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/ttl
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyTimeToLive extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'TTL';
    }
}

/**
 * @link http://redis.io/commands/rename
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyRename extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RENAME';
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/expireat
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyExpireAt extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EXPIREAT';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/keys
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyKeys extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'KEYS';
    }
}

/**
 * @link http://redis.io/commands/zrangebyscore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRangeByScore extends ZSetRange
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZRANGEBYSCORE';
    }

    /**
     * {@inheritdoc}
     */
    protected function prepareOptions($options)
    {
        $opts = array_change_key_case($options, CASE_UPPER);
        $finalizedOpts = array();

        if (isset($opts['LIMIT']) && is_array($opts['LIMIT'])) {
            $limit = array_change_key_case($opts['LIMIT'], CASE_UPPER);

            $finalizedOpts[] = 'LIMIT';
            $finalizedOpts[] = isset($limit['OFFSET']) ? $limit['OFFSET'] : $limit[0];
            $finalizedOpts[] = isset($limit['COUNT']) ? $limit['COUNT'] : $limit[1];
        }

        return array_merge($finalizedOpts, parent::prepareOptions($options));
    }

    /**
     * {@inheritdoc}
     */
    protected function withScores()
    {
        $arguments = $this->getArguments();

        for ($i = 3; $i < count($arguments); $i++) {
            switch (strtoupper($arguments[$i])) {
                case 'WITHSCORES':
                    return true;

                case 'LIMIT':
                    $i += 2;
                    break;
            }
        }

        return false;
    }
}

/**
 * @link http://redis.io/commands/mset
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetMultiple extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MSET';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 1 && is_array($arguments[0])) {
            $flattenedKVs = array();
            $args = $arguments[0];

            foreach ($args as $k => $v) {
                $flattenedKVs[] = $k;
                $flattenedKVs[] = $v;
            }

            return $flattenedKVs;
        }

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::interleaved($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/expire
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyExpire extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EXPIRE';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/info
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerInfo extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'INFO';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        $info      = array();
        $infoLines = preg_split('/\r?\n/', $data);

        foreach ($infoLines as $row) {
            @list($k, $v) = explode(':', $row);

            if ($row === '' || !isset($v)) {
                continue;
            }

            if (!preg_match('/^db\d+$/', $k)) {
                if ($k === 'allocation_stats') {
                    $info[$k] = $this->parseAllocationStats($v);
                    continue;
                }

                $info[$k] = $v;
            } else {
                $info[$k] = $this->parseDatabaseStats($v);
            }
        }

        return $info;
    }

    /**
     * Parses the reply buffer and extracts the statistics of each logical DB.
     *
     * @param string $str Reply buffer.
     * @return array
     */
    protected function parseDatabaseStats($str)
    {
        $db = array();

        foreach (explode(',', $str) as $dbvar) {
            list($dbvk, $dbvv) = explode('=', $dbvar);
            $db[trim($dbvk)] = $dbvv;
        }

        return $db;
    }

    /**
     * Parses the reply buffer and extracts the allocation statistics.
     *
     * @param string $str Reply buffer.
     * @return array
     */
    protected function parseAllocationStats($str)
    {
        $stats = array();

        foreach (explode(',', $str) as $kv) {
            @list($size, $objects, $extra) = explode('=', $kv);

            // hack to prevent incorrect values when parsing the >=256 key
            if (isset($extra)) {
                $size = ">=$objects";
                $objects = $extra;
            }

            $stats[$size] = $objects;
        }

        return $stats;
    }
}

/**
 * @link http://redis.io/commands/zunionstore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetUnionStore extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZUNIONSTORE';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        $options = array();
        $argc = count($arguments);

        if ($argc > 2 && is_array($arguments[$argc - 1])) {
            $options = $this->prepareOptions(array_pop($arguments));
        }

        if (is_array($arguments[1])) {
            $arguments = array_merge(
                array($arguments[0], count($arguments[1])),
                $arguments[1]
            );
        }

        return array_merge($arguments, $options);
    }

    /**
     * Returns a list of options and modifiers compatible with Redis.
     *
     * @param array $options List of options.
     * @return array
     */
    private function prepareOptions($options)
    {
        $opts = array_change_key_case($options, CASE_UPPER);
        $finalizedOpts = array();

        if (isset($opts['WEIGHTS']) && is_array($opts['WEIGHTS'])) {
            $finalizedOpts[] = 'WEIGHTS';

            foreach ($opts['WEIGHTS'] as $weight) {
                $finalizedOpts[] = $weight;
            }
        }

        if (isset($opts['AGGREGATE'])) {
            $finalizedOpts[] = 'AGGREGATE';
            $finalizedOpts[] = $opts['AGGREGATE'];
        }

        return $finalizedOpts;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        if ($arguments = $this->getArguments()) {
            $arguments[0] = "$prefix{$arguments[0]}";
            $length = ((int) $arguments[1]) + 2;

            for ($i = 2; $i < $length; $i++) {
                $arguments[$i] = "$prefix{$arguments[$i]}";
            }

            $this->setRawArguments($arguments);
        }
    }
}

/**
 * @link http://redis.io/commands/unsubscribe
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubUnsubscribe extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'UNSUBSCRIBE';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeArguments($arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/subscribe
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubSubscribe extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SUBSCRIBE';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeArguments($arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/setex
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetExpire extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SETEX';
    }
}

/**
 * @link http://redis.io/commands/evalsha
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerEvalSHA extends ServerEval
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EVALSHA';
    }

    /**
     * Returns the SHA1 hash of the body of the script.
     *
     * @return string SHA1 hash.
     */
    public function getScriptHash()
    {
        return $this->getArgument(0);
    }
}

/**
 * @link http://redis.io/commands/decrby
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringDecrementBy extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DECRBY';
    }
}

/**
 * @link http://redis.io/commands/get
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringGet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'GET';
    }
}

/**
 * @link http://redis.io/commands/decr
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringDecrement extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DECR';
    }
}

/**
 * @link http://redis.io/commands/bitop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringBitOp extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BITOP';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 3 && is_array($arguments[2])) {
            list($operation, $destination, ) = $arguments;
            $arguments = $arguments[2];
            array_unshift($arguments, $operation, $destination);
        }

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::skipFirst($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/append
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringAppend extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'APPEND';
    }
}

/**
 * @link http://redis.io/commands/bitcount
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringBitCount extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BITCOUNT';
    }
}

/**
 * @link http://redis.io/commands/getbit
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringGetBit extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'GETBIT';
    }
}

/**
 * @link http://redis.io/commands/mget
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringGetMultiple extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MGET';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeArguments($arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/incrbyfloat
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringIncrementByFloat extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'INCRBYFLOAT';
    }
}

/**
 * @link http://redis.io/commands/psetex
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringPreciseSetExpire extends StringSetExpire
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PSETEX';
    }
}

/**
 * @link http://redis.io/commands/incrby
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringIncrementBy extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'INCRBY';
    }
}

/**
 * @link http://redis.io/commands/incr
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringIncrement extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'INCR';
    }
}

/**
 * @link http://redis.io/commands/getrange
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringGetRange extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'GETRANGE';
    }
}

/**
 * @link http://redis.io/commands/getset
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringGetSet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'GETSET';
    }
}

/**
 * @link http://redis.io/commands/sunionstore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetUnionStore extends SetIntersectionStore
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SUNIONSTORE';
    }
}

/**
 * @link http://redis.io/commands/sunion
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetUnion extends SetIntersection
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SUNION';
    }
}

/**
 * @link http://redis.io/commands/sdiffstore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetDifferenceStore extends SetIntersectionStore
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SDIFFSTORE';
    }
}

/**
 * @link http://redis.io/commands/hexists
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashExists extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HEXISTS';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/sdiff
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetDifference extends SetIntersection
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SDIFF';
    }
}

/**
 * @link http://redis.io/commands/scard
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetCardinality extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SCARD';
    }
}

/**
 * @link http://redis.io/commands/time
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerTime extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'TIME';
    }
}

/**
 * @link http://redis.io/commands/sadd
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetAdd extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SADD';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/hdel
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashDelete extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HDEL';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/sismember
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetIsMember extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SISMEMBER';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/srandmember
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetRandomMember extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SRANDMEMBER';
    }
}

/**
 * @link http://redis.io/commands/srem
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetRemove extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SREM';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/spop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetPop extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SPOP';
    }
}

/**
 * @link http://redis.io/commands/smove
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetMove extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SMOVE';
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::skipLast($this, $prefix);
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/smembers
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SetMembers extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SMEMBERS';
    }
}

/**
 * @link http://redis.io/commands/set
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SET';
    }
}

/**
 * @link http://redis.io/commands/setbit
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetBit extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SETBIT';
    }
}

/**
 * @link http://redis.io/commands/zrank
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRank extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZRANK';
    }
}

/**
 * @link http://redis.io/commands/zrem
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRemove extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREM';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/echo
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionEcho extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ECHO';
    }
}

/**
 * @link http://redis.io/commands/ping
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionPing extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PING';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return $data === 'PONG' ? true : false;
    }
}

/**
 * @link http://redis.io/commands/zincrby
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetIncrementBy extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZINCRBY';
    }
}

/**
 * @link http://redis.io/commands/zinterstore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetIntersectionStore extends ZSetUnionStore
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZINTERSTORE';
    }
}

/**
 * @link http://redis.io/commands/zremrangebyrank
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRemoveRangeByRank extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREMRANGEBYRANK';
    }
}

/**
 * @link http://redis.io/commands/zremrangebyscore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetRemoveRangeByScore extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREMRANGEBYSCORE';
    }
}

/**
 * @link http://redis.io/commands/zscore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetScore extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZSCORE';
    }
}

/**
 * @link http://redis.io/commands/auth
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionAuth extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'AUTH';
    }
}

/**
 * @link http://redis.io/commands/zrevrank
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetReverseRank extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREVRANK';
    }
}

/**
 * @link http://redis.io/commands/zrevrangebyscore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetReverseRangeByScore extends ZSetRangeByScore
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREVRANGEBYSCORE';
    }
}

/**
 * @link http://redis.io/commands/zrevrange
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetReverseRange extends ZSetRange
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZREVRANGE';
    }
}

/**
 * @link http://redis.io/commands/zcount
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetCount extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZCOUNT';
    }
}

/**
 * @link http://redis.io/commands/zcard
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetCardinality extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZCARD';
    }
}

/**
 * @link http://redis.io/commands/setrange
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetRange extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SETRANGE';
    }
}

/**
 * @link http://redis.io/commands/strlen
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringStrlen extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'STRLEN';
    }
}

/**
 * @link http://redis.io/commands/setnx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetPreserve extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SETNX';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/msetnx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSetMultiplePreserve extends StringSetMultiple
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MSETNX';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/select
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionSelect extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SELECT';
    }
}

/**
 * @link http://redis.io/commands/quit
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionQuit extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'QUIT';
    }
}

/**
 * @link http://redis.io/commands/substr
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StringSubstr extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SUBSTR';
    }
}

/**
 * @link http://redis.io/commands/discard
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TransactionDiscard extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DISCARD';
    }
}

/**
 * @link http://redis.io/commands/watch
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TransactionWatch extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'WATCH';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (isset($arguments[0]) && is_array($arguments[0])) {
            return $arguments[0];
        }

        return $arguments;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/zadd
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ZSetAdd extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'ZADD';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[1])) {
            $flattened = array($arguments[0]);

            foreach($arguments[1] as $member => $score) {
                $flattened[] = $score;
                $flattened[] = $member;
            }

            return $flattened;
        }

        return $arguments;
    }
}

/**
 * @link http://redis.io/commands/unwatch
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TransactionUnwatch extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'UNWATCH';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/multi
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TransactionMulti extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MULTI';
    }
}

/**
 * @link http://redis.io/commands/exec
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TransactionExec extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EXEC';
    }
}

/**
 * @link http://redis.io/commands/slowlog
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerSlowlog extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SLOWLOG';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        if (is_array($data)) {
            $log = array();

            foreach ($data as $index => $entry) {
                $log[$index] = array(
                    'id' => $entry[0],
                    'timestamp' => $entry[1],
                    'duration' => $entry[2],
                    'command' => $entry[3],
                );
            }

            return $log;
        }

        return $data;
    }
}

/**
 * @link http://redis.io/commands/slaveof
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerSlaveOf extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SLAVEOF';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 0 || $arguments[0] === 'NO ONE') {
            return array('NO', 'ONE');
        }

        return $arguments;
    }
}

/**
 * @link http://redis.io/commands/hmset
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashSetMultiple extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HMSET';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[1])) {
            $flattenedKVs = array($arguments[0]);
            $args = $arguments[1];

            foreach ($args as $k => $v) {
                $flattenedKVs[] = $k;
                $flattenedKVs[] = $v;
            }

            return $flattenedKVs;
        }

        return $arguments;
    }
}

/**
 * @link http://redis.io/commands/rpop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopLast extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RPOP';
    }
}

/**
 * @link http://redis.io/commands/lpop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopFirst extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LPOP';
    }
}

/**
 * @link http://redis.io/commands/llen
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListLength extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LLEN';
    }
}

/**
 * @link http://redis.io/commands/lindex
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListIndex extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LINDEX';
    }
}

/**
 * @link http://redis.io/commands/linsert
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListInsert extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LINSERT';
    }
}

/**
 * @link http://redis.io/commands/brpop
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopLastBlocking extends ListPopFirstBlocking
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BRPOP';
    }
}

/**
 * @link http://redis.io/commands/rpoplpush
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopLastPushHead extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RPOPLPUSH';
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/hset
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashSet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HSET';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/rpushx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPushTailX extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RPUSHX';
    }
}

/**
 * @link http://redis.io/commands/lpushx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPushHeadX extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LPUSHX';
    }
}

/**
 * @link http://redis.io/commands/lpush
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPushHead extends ListPushTail
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LPUSH';
    }
}

/**
 * @link http://redis.io/commands/brpoplpush
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListPopLastPushHeadBlocking extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BRPOPLPUSH';
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::skipLast($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/type
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyType extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'TYPE';
    }
}

/**
 * @link http://redis.io/commands/hsetnx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashSetPreserve extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HSETNX';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/persist
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyPersist extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PERSIST';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/pexpire
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyPreciseExpire extends KeyExpire
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PEXPIRE';
    }
}

/**
 * @link http://redis.io/commands/move
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyMove extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MOVE';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/keys
 * @author Daniele Alessandri <suppakilla@gmail.com>
 * @deprecated
 */
class KeyKeysV12x extends KeyKeys
{
    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return explode(' ', $data);
    }
}

/**
 * @link http://redis.io/commands/dump
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyDump extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DUMP';
    }
}

/**
 * @link http://redis.io/commands/del
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyDelete extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DEL';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeArguments($arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        PrefixHelpers::all($this, $prefix);
    }
}

/**
 * @link http://redis.io/commands/pexpireat
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyPreciseExpireAt extends KeyExpireAt
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PEXPIREAT';
    }
}

/**
 * @link http://redis.io/commands/pttl
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyPreciseTimeToLive extends KeyTimeToLive
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PTTL';
    }
}

/**
 * @link http://redis.io/commands/restore
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyRestore extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RESTORE';
    }
}

/**
 * @link http://redis.io/commands/sort
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeySort extends AbstractCommand implements PrefixableCommandInterface
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SORT';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (count($arguments) === 1) {
            return $arguments;
        }

        $query = array($arguments[0]);
        $sortParams = array_change_key_case($arguments[1], CASE_UPPER);

        if (isset($sortParams['BY'])) {
            $query[] = 'BY';
            $query[] = $sortParams['BY'];
        }

        if (isset($sortParams['GET'])) {
            $getargs = $sortParams['GET'];

            if (is_array($getargs)) {
                foreach ($getargs as $getarg) {
                    $query[] = 'GET';
                    $query[] = $getarg;
                }
            } else {
                $query[] = 'GET';
                $query[] = $getargs;
            }
        }

        if (isset($sortParams['LIMIT']) &&
            is_array($sortParams['LIMIT']) &&
            count($sortParams['LIMIT']) == 2) {

            $query[] = 'LIMIT';
            $query[] = $sortParams['LIMIT'][0];
            $query[] = $sortParams['LIMIT'][1];
        }

        if (isset($sortParams['SORT'])) {
            $query[] = strtoupper($sortParams['SORT']);
        }

        if (isset($sortParams['ALPHA']) && $sortParams['ALPHA'] == true) {
            $query[] = 'ALPHA';
        }

        if (isset($sortParams['STORE'])) {
            $query[] = 'STORE';
            $query[] = $sortParams['STORE'];
        }

        return $query;
    }

    /**
     * {@inheritdoc}
     */
    public function prefixKeys($prefix)
    {
        if ($arguments = $this->getArguments()) {
            $arguments[0] = "$prefix{$arguments[0]}";

            if (($count = count($arguments)) > 1) {
                for ($i = 1; $i < $count; $i++) {
                    switch ($arguments[$i]) {
                        case 'BY':
                        case 'STORE':
                            $arguments[$i] = "$prefix{$arguments[++$i]}";
                            break;

                        case 'GET':
                            $value = $arguments[++$i];
                            if ($value !== '#') {
                                $arguments[$i] = "$prefix$value";
                            }
                            break;

                        case 'LIMIT';
                            $i += 2;
                            break;
                    }
                }
            }

            $this->setRawArguments($arguments);
        }
    }
}

/**
 * @link http://redis.io/commands/renamenx
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyRenamePreserve extends KeyRename
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RENAMENX';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/**
 * @link http://redis.io/commands/hvals
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashValues extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HVALS';
    }
}

/**
 * @link http://redis.io/commands/randomkey
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyRandom extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'RANDOMKEY';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return $data !== '' ? $data : null;
    }
}

/**
 * @link http://redis.io/commands/lrange
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListRange extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LRANGE';
    }
}

/**
 * @link http://redis.io/commands/lrem
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListRemove extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LREM';
    }
}

/**
 * @link http://redis.io/commands/flushdb
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerFlushDatabase extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'FLUSHDB';
    }
}

/**
 * @link http://redis.io/commands/hget
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashGet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HGET';
    }
}

/**
 * @link http://redis.io/commands/flushall
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerFlushAll extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'FLUSHALL';
    }
}

/**
 * @link http://redis.io/commands/hgetall
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashGetAll extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HGETALL';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        $result = array();

        for ($i = 0; $i < count($data); $i++) {
            $result[$data[$i]] = $data[++$i];
        }

        return $result;
    }
}

/**
 * @link http://redis.io/commands/dbsize
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerDatabaseSize extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'DBSIZE';
    }
}

/**
 * @link http://redis.io/commands/hmget
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashGetMultiple extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HMGET';
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        return self::normalizeVariadic($arguments);
    }
}

/**
 * @link http://redis.io/commands/info
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerInfoV26x extends ServerInfo
{
    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        $info = array();
        $current = null;
        $infoLines = preg_split('/\r?\n/', $data);

        if (isset($infoLines[0]) && $infoLines[0][0] !== '#') {
            return parent::parseResponse($data);
        }

        foreach ($infoLines as $row) {
            if ($row === '') {
                continue;
            }

            if (preg_match('/^# (\w+)$/', $row, $matches)) {
                $info[$matches[1]] = array();
                $current = &$info[$matches[1]];
                continue;
            }

            list($k, $v) = explode(':', $row);

            if (!preg_match('/^db\d+$/', $k)) {
                if ($k === 'allocation_stats') {
                    $current[$k] = $this->parseAllocationStats($v);
                    continue;
                }

                $current[$k] = $v;
            } else {
                $current[$k] = $this->parseDatabaseStats($v);
            }
        }

        return $info;
    }
}

/**
 * @link http://redis.io/commands/lastsave
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerLastSave extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LASTSAVE';
    }
}

/**
 * @link http://redis.io/commands/script
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerScript extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SCRIPT';
    }
}

/**
 * @link http://redis.io/commands/shutdown
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerShutdown extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SHUTDOWN';
    }
}

/**
 * @link http://redis.io/commands/save
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerSave extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'SAVE';
    }
}

/**
 * @link http://redis.io/commands/object
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerObject extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'OBJECT';
    }
}

/**
 * @link http://redis.io/commands/monitor
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerMonitor extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'MONITOR';
    }
}

/**
 * @link http://redis.io/commands/config-set
 * @link http://redis.io/commands/config-get
 * @link http://redis.io/commands/config-resetstat
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerConfig extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'CONFIG';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        if (is_array($data)) {
            $result = array();

            for ($i = 0; $i < count($data); $i++) {
                $result[$data[$i]] = $data[++$i];
            }

            return $result;
        }

        return $data;
    }
}

/**
 * @link http://redis.io/commands/client
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerClient extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'CLIENT';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        $args = array_change_key_case($this->getArguments(), CASE_UPPER);

        switch (strtoupper($args[0])) {
            case 'LIST':
                return $this->parseClientList($data);
            case 'KILL':
            case 'GETNAME':
            case 'SETNAME':
            default:
                return $data;
        }
    }

    /**
     * Parses the reply buffer and returns the list of clients returned by
     * the CLIENT LIST command.
     *
     * @param string $data Reply buffer
     * @return array
     */
    protected function parseClientList($data)
    {
        $clients = array();

        foreach (explode("\n", $data, -1) as $clientData) {
            $client = array();

            foreach (explode(' ', $clientData) as $kv) {
                @list($k, $v) = explode('=', $kv);
                $client[$k] = $v;
            }

            $clients[] = $client;
        }

        return $clients;
    }
}

/**
 * Class that defines a few helpers method for prefixing keys.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PrefixHelpers
{
    /**
     * Applies the specified prefix only the first argument.
     *
     * @param CommandInterface $command Command instance.
     * @param string $prefix Prefix string.
     */
    public static function first(CommandInterface $command, $prefix)
    {
        if ($arguments = $command->getArguments()) {
            $arguments[0] = "$prefix{$arguments[0]}";
            $command->setRawArguments($arguments);
        }
    }

    /**
     * Applies the specified prefix to all the arguments.
     *
     * @param CommandInterface $command Command instance.
     * @param string $prefix Prefix string.
     */
    public static function all(CommandInterface $command, $prefix)
    {
        if ($arguments = $command->getArguments()) {
            foreach ($arguments as &$key) {
                $key = "$prefix$key";
            }

            $command->setRawArguments($arguments);
        }
    }

    /**
     * Applies the specified prefix only to even arguments in the list.
     *
     * @param CommandInterface $command Command instance.
     * @param string $prefix Prefix string.
     */
    public static function interleaved(CommandInterface $command, $prefix)
    {
        if ($arguments = $command->getArguments()) {
            $length = count($arguments);

            for ($i = 0; $i < $length; $i += 2) {
                $arguments[$i] = "$prefix{$arguments[$i]}";
            }

            $command->setRawArguments($arguments);
        }
    }

    /**
     * Applies the specified prefix to all the arguments but the first one.
     *
     * @param CommandInterface $command Command instance.
     * @param string $prefix Prefix string.
     */
    public static function skipFirst(CommandInterface $command, $prefix)
    {
        if ($arguments = $command->getArguments()) {
            $length = count($arguments);

            for ($i = 1; $i < $length; $i++) {
                $arguments[$i] = "$prefix{$arguments[$i]}";
            }

            $command->setRawArguments($arguments);
        }
    }

    /**
     * Applies the specified prefix to all the arguments but the last one.
     *
     * @param CommandInterface $command Command instance.
     * @param string $prefix Prefix string.
     */
    public static function skipLast(CommandInterface $command, $prefix)
    {
        if ($arguments = $command->getArguments()) {
            $length = count($arguments);

            for ($i = 0; $i < $length - 1; $i++) {
                $arguments[$i] = "$prefix{$arguments[$i]}";
            }

            $command->setRawArguments($arguments);
        }
    }
}

/**
 * @link http://redis.io/commands/publish
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubPublish extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PUBLISH';
    }
}

/**
 * @link http://redis.io/commands/hkeys
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashKeys extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HKEYS';
    }
}

/**
 * @link http://redis.io/commands/hlen
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashLength extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HLEN';
    }
}

/**
 * @link http://redis.io/commands/lset
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListSet extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LSET';
    }
}

/**
 * @link http://redis.io/commands/ltrim
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ListTrim extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'LTRIM';
    }
}

/**
 * @link http://redis.io/commands/hincrbyfloat
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashIncrementByFloat extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HINCRBYFLOAT';
    }
}

/**
 * @link http://redis.io/commands/psubscribe
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubSubscribeByPattern extends PubSubSubscribe
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PSUBSCRIBE';
    }
}

/**
 * @link http://redis.io/commands/bgrewriteaof
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerBackgroundRewriteAOF extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BGREWRITEAOF';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return $data == 'Background append only file rewriting started';
    }
}

/**
 * @link http://redis.io/commands/bgsave
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerBackgroundSave extends AbstractCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'BGSAVE';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return $data === 'Background saving started' ? true : $data;
    }
}

/**
 * Base class used to implement an higher level abstraction for "virtual"
 * commands based on EVAL.
 *
 * @link http://redis.io/commands/eval
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class ScriptedCommand extends ServerEvalSHA
{
    /**
     * Gets the body of a Lua script.
     *
     * @return string
     */
    public abstract function getScript();

    /**
     * Specifies the number of arguments that should be considered as keys.
     *
     * The default behaviour for the base class is to return 0 to indicate that
     * all the elements of the arguments array should be considered as keys, but
     * subclasses can enforce a static number of keys.
     *
     * @return int
     */
    protected function getKeysCount()
    {
        return 0;
    }

    /**
     * Returns the elements from the arguments that are identified as keys.
     *
     * @return array
     */
    public function getKeys()
    {
        return array_slice($this->getArguments(), 2, $this->getKeysCount());
    }

    /**
     * {@inheritdoc}
     */
    protected function filterArguments(Array $arguments)
    {
        if (($numkeys = $this->getKeysCount()) && $numkeys < 0) {
            $numkeys = count($arguments) + $numkeys;
        }

        return array_merge(array(sha1($this->getScript()), (int) $numkeys), $arguments);
    }

    /**
     * @return array
     */
    public function getEvalArguments()
    {
        $arguments = $this->getArguments();
        $arguments[0] = $this->getScript();

        return $arguments;
    }
}

/**
 * @link http://redis.io/commands/punsubscribe
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubUnsubscribeByPattern extends PubSubUnsubscribe
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'PUNSUBSCRIBE';
    }
}

/**
 * @link http://redis.io/commands/hincrby
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class HashIncrementBy extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'HINCRBY';
    }
}

/**
 * @link http://redis.io/commands/exists
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyExists extends PrefixableCommand
{
    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'EXISTS';
    }

    /**
     * {@inheritdoc}
     */
    public function parseResponse($data)
    {
        return (bool) $data;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Connection;

use Predis\ClientException;
use Predis\CommunicationException;
use Predis\NotSupportedException;
use Predis\Command\CommandInterface;
use Predis\Protocol\ProtocolException;
use Predis\Protocol\ProtocolInterface;
use Predis\Protocol\Text\TextProtocol;
use Predis\Profile\ServerProfile;
use Predis\Profile\ServerProfileInterface;
use Predis\Replication\ReplicationStrategy;
use Predis\ResponseError;
use Predis\ResponseQueued;
use Predis\Cluster\CommandHashStrategyInterface;
use Predis\Cluster\PredisClusterHashStrategy;
use Predis\Cluster\Distribution\DistributionStrategyInterface;
use Predis\Cluster\Distribution\HashRing;
use Predis\ResponseErrorInterface;
use Predis\Cluster\RedisClusterHashStrategy;
use Predis\Iterator\MultiBulkResponseSimple;
use Predis\Connection\ConnectionException;

/**
 * Defines a connection object used to communicate with one or multiple
 * Redis servers.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ConnectionInterface
{
    /**
     * Opens the connection.
     */
    public function connect();

    /**
     * Closes the connection.
     */
    public function disconnect();

    /**
     * Returns if the connection is open.
     *
     * @return Boolean
     */
    public function isConnected();

    /**
     * Write a Redis command on the connection.
     *
     * @param CommandInterface $command Instance of a Redis command.
     */
    public function writeCommand(CommandInterface $command);

    /**
     * Reads the reply for a Redis command from the connection.
     *
     * @param CommandInterface $command Instance of a Redis command.
     * @return mixed
     */
    public function readResponse(CommandInterface $command);

    /**
     * Writes a Redis command to the connection and reads back the reply.
     *
     * @param CommandInterface $command Instance of a Redis command.
     * @return mixed
     */
    public function executeCommand(CommandInterface $command);
}

/**
 * Defines a connection object used to communicate with a single Redis server.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface SingleConnectionInterface extends ConnectionInterface
{
    /**
     * Returns a string representation of the connection.
     *
     * @return string
     */
    public function __toString();

    /**
     * Returns the underlying resource used to communicate with a Redis server.
     *
     * @return mixed
     */
    public function getResource();

    /**
     * Gets the parameters used to initialize the connection object.
     *
     * @return ConnectionParametersInterface
     */
    public function getParameters();

    /**
     * Pushes the instance of a Redis command to the queue of commands executed
     * when the actual connection to a server is estabilished.
     *
     * @param CommandInterface $command Instance of a Redis command.
     */
    public function pushInitCommand(CommandInterface $command);

    /**
     * Reads a reply from the server.
     *
     * @return mixed
     */
    public function read();
}

/**
 * Defines a virtual connection composed by multiple connection objects.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface AggregatedConnectionInterface extends ConnectionInterface
{
    /**
     * Adds a connection instance to the aggregated connection.
     *
     * @param SingleConnectionInterface $connection Instance of a connection.
     */
    public function add(SingleConnectionInterface $connection);

    /**
     * Removes the specified connection instance from the aggregated
     * connection.
     *
     * @param SingleConnectionInterface $connection Instance of a connection.
     * @return Boolean Returns true if the connection was in the pool.
     */
    public function remove(SingleConnectionInterface $connection);

    /**
     * Gets the actual connection instance in charge of the specified command.
     *
     * @param CommandInterface $command Instance of a Redis command.
     * @return SingleConnectionInterface
     */
    public function getConnection(CommandInterface $command);

    /**
     * Retrieves a connection instance from the aggregated connection
     * using an alias.
     *
     * @param string $connectionId Alias of a connection
     * @return SingleConnectionInterface
     */
    public function getConnectionById($connectionId);
}

/**
 * Base class with the common logic used by connection classes to communicate with Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class AbstractConnection implements SingleConnectionInterface
{
    private $resource;
    private $cachedId;

    protected $parameters;
    protected $initCmds = array();

    /**
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     */
    public function __construct(ConnectionParametersInterface $parameters)
    {
        $this->parameters = $this->checkParameters($parameters);
    }

    /**
     * Disconnects from the server and destroys the underlying resource when
     * PHP's garbage collector kicks in.
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Checks some of the parameters used to initialize the connection.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     */
    protected function checkParameters(ConnectionParametersInterface $parameters)
    {
        switch ($parameters->scheme) {
            case 'unix':
                if (!isset($parameters->path)) {
                    throw new \InvalidArgumentException('Missing UNIX domain socket path');
                }

            case 'tcp':
                return $parameters;

            default:
                throw new \InvalidArgumentException("Invalid scheme: {$parameters->scheme}");
        }
    }

    /**
     * Creates the underlying resource used to communicate with Redis.
     *
     * @return mixed
     */
    protected abstract function createResource();

    /**
     * {@inheritdoc}
     */
    public function isConnected()
    {
        return isset($this->resource);
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        if ($this->isConnected()) {
            throw new ClientException('Connection already estabilished');
        }

        $this->resource = $this->createResource();
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        unset($this->resource);
    }

    /**
     * {@inheritdoc}
     */
    public function pushInitCommand(CommandInterface $command)
    {
        $this->initCmds[] = $command;
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        $this->writeCommand($command);
        return $this->readResponse($command);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        return $this->read();
    }

    /**
     * Helper method to handle connection errors.
     *
     * @param string $message Error message.
     * @param int $code Error code.
     */
    protected function onConnectionError($message, $code = null)
    {
        CommunicationException::handle(new ConnectionException($this, "$message [{$this->parameters->scheme}://{$this->getIdentifier()}]", $code));
    }

    /**
     * Helper method to handle protocol errors.
     *
     * @param string $message Error message.
     */
    protected function onProtocolError($message)
    {
        CommunicationException::handle(new ProtocolException($this, "$message [{$this->parameters->scheme}://{$this->getIdentifier()}]"));
    }

    /**
     * Helper method to handle not supported connection parameters.
     *
     * @param string $option Name of the option.
     * @param mixed $parameters Parameters used to initialize the connection.
     */
    protected function onInvalidOption($option, $parameters = null)
    {
        $class = get_called_class();
        $message = "Invalid option for connection $class: $option";

        if (isset($parameters)) {
            $message .= sprintf(' [%s => %s]', $option, $parameters->{$option});
        }

        throw new NotSupportedException($message);
    }

    /**
     * {@inheritdoc}
     */
    public function getResource()
    {
        if (isset($this->resource)) {
            return $this->resource;
        }

        $this->connect();

        return $this->resource;
    }

    /**
     * {@inheritdoc}
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * Gets an identifier for the connection.
     *
     * @return string
     */
    protected function getIdentifier()
    {
        if ($this->parameters->scheme === 'unix') {
            return $this->parameters->path;
        }

        return "{$this->parameters->host}:{$this->parameters->port}";
    }

    /**
     * {@inheritdoc}
     */
    public function __toString()
    {
        if (!isset($this->cachedId)) {
            $this->cachedId = $this->getIdentifier();
        }

        return $this->cachedId;
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array('parameters', 'initCmds');
    }
}

/**
 * Standard connection to Redis servers implemented on top of PHP's streams.
 * The connection parameters supported by this class are:
 *
 *  - scheme: it can be either 'tcp' or 'unix'.
 *  - host: hostname or IP address of the server.
 *  - port: TCP port of the server.
 *  - timeout: timeout to perform the connection.
 *  - read_write_timeout: timeout of read / write operations.
 *  - async_connect: performs the connection asynchronously.
 *  - tcp_nodelay: enables or disables Nagle's algorithm for coalescing.
 *  - persistent: the connection is left intact after a GC collection.
 *  - iterable_multibulk: multibulk replies treated as iterable objects.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StreamConnection extends AbstractConnection
{
    private $mbiterable;

    /**
     * {@inheritdoc}
     */
    public function __construct(ConnectionParametersInterface $parameters)
    {
        $this->mbiterable = (bool) $parameters->iterable_multibulk;

        parent::__construct($parameters);
    }

    /**
     * Disconnects from the server and destroys the underlying resource when
     * PHP's garbage collector kicks in only if the connection has not been
     * marked as persistent.
     */
    public function __destruct()
    {
        if (isset($this->parameters) && !$this->parameters->persistent) {
            $this->disconnect();
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function createResource()
    {
        $parameters = $this->parameters;
        $initializer = "{$parameters->scheme}StreamInitializer";

        return $this->$initializer($parameters);
    }

    /**
     * Initializes a TCP stream resource.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return resource
     */
    private function tcpStreamInitializer(ConnectionParametersInterface $parameters)
    {
        $uri = "tcp://{$parameters->host}:{$parameters->port}/";
        $flags = STREAM_CLIENT_CONNECT;

        if (isset($parameters->async_connect) && $parameters->async_connect) {
            $flags |= STREAM_CLIENT_ASYNC_CONNECT;
        }
        if (isset($parameters->persistent) && $parameters->persistent) {
            $flags |= STREAM_CLIENT_PERSISTENT;
        }

        $resource = @stream_socket_client($uri, $errno, $errstr, $parameters->timeout, $flags);

        if (!$resource) {
            $this->onConnectionError(trim($errstr), $errno);
        }

        if (isset($parameters->read_write_timeout)) {
            $rwtimeout = $parameters->read_write_timeout;
            $rwtimeout = $rwtimeout > 0 ? $rwtimeout : -1;
            $timeoutSeconds  = floor($rwtimeout);
            $timeoutUSeconds = ($rwtimeout - $timeoutSeconds) * 1000000;
            stream_set_timeout($resource, $timeoutSeconds, $timeoutUSeconds);
        }

        if (isset($parameters->tcp_nodelay) && version_compare(PHP_VERSION, '5.4.0') >= 0) {
            $socket = socket_import_stream($resource);
            socket_set_option($socket, SOL_TCP, TCP_NODELAY, (int) $parameters->tcp_nodelay);
        }

        return $resource;
    }

    /**
     * Initializes a UNIX stream resource.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return resource
     */
    private function unixStreamInitializer(ConnectionParametersInterface $parameters)
    {
        $uri = "unix://{$parameters->path}";
        $flags = STREAM_CLIENT_CONNECT;

        if ($parameters->persistent) {
            $flags |= STREAM_CLIENT_PERSISTENT;
        }

        $resource = @stream_socket_client($uri, $errno, $errstr, $parameters->timeout, $flags);

        if (!$resource) {
            $this->onConnectionError(trim($errstr), $errno);
        }

        return $resource;
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        parent::connect();

        if ($this->initCmds) {
            $this->sendInitializationCommands();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        if ($this->isConnected()) {
            fclose($this->getResource());
            parent::disconnect();
        }
    }

    /**
     * Sends the initialization commands to Redis when the connection is opened.
     */
    private function sendInitializationCommands()
    {
        foreach ($this->initCmds as $command) {
            $this->writeCommand($command);
        }
        foreach ($this->initCmds as $command) {
            $this->readResponse($command);
        }
    }

    /**
     * Performs a write operation on the stream of the buffer containing a
     * command serialized with the Redis wire protocol.
     *
     * @param string $buffer Redis wire protocol representation of a command.
     */
    protected function writeBytes($buffer)
    {
        $socket = $this->getResource();

        while (($length = strlen($buffer)) > 0) {
            $written = fwrite($socket, $buffer);

            if ($length === $written) {
                return;
            }
            if ($written === false || $written === 0) {
                $this->onConnectionError('Error while writing bytes to the server');
            }

            $buffer = substr($buffer, $written);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        $socket = $this->getResource();
        $chunk  = fgets($socket);

        if ($chunk === false || $chunk === '') {
            $this->onConnectionError('Error while reading line from the server');
        }

        $prefix  = $chunk[0];
        $payload = substr($chunk, 1, -2);

        switch ($prefix) {
            case '+':    // inline
                switch ($payload) {
                    case 'OK':
                        return true;

                    case 'QUEUED':
                        return new ResponseQueued();

                    default:
                        return $payload;
                }

            case '$':    // bulk
                $size = (int) $payload;
                if ($size === -1) {
                    return null;
                }

                $bulkData = '';
                $bytesLeft = ($size += 2);

                do {
                    $chunk = fread($socket, min($bytesLeft, 4096));

                    if ($chunk === false || $chunk === '') {
                        $this->onConnectionError('Error while reading bytes from the server');
                    }

                    $bulkData .= $chunk;
                    $bytesLeft = $size - strlen($bulkData);
                } while ($bytesLeft > 0);

                return substr($bulkData, 0, -2);

            case '*':    // multi bulk
                $count = (int) $payload;

                if ($count === -1) {
                    return null;
                }
                if ($this->mbiterable) {
                    return new MultiBulkResponseSimple($this, $count);
                }

                $multibulk = array();

                for ($i = 0; $i < $count; $i++) {
                    $multibulk[$i] = $this->read();
                }

                return $multibulk;

            case ':':    // integer
                return (int) $payload;

            case '-':    // error
                return new ResponseError($payload);

            default:
                $this->onProtocolError("Unknown prefix: '$prefix'");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $commandId = $command->getId();
        $arguments = $command->getArguments();

        $cmdlen = strlen($commandId);
        $reqlen = count($arguments) + 1;

        $buffer = "*{$reqlen}\r\n\${$cmdlen}\r\n{$commandId}\r\n";

        for ($i = 0; $i < $reqlen - 1; $i++) {
            $argument = $arguments[$i];
            $arglen = strlen($argument);
            $buffer .= "\${$arglen}\r\n{$argument}\r\n";
        }

        $this->writeBytes($buffer);
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array_merge(parent::__sleep(), array('mbiterable'));
    }
}

/**
 * Defines a cluster of Redis servers formed by aggregating multiple
 * connection objects.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ClusterConnectionInterface extends AggregatedConnectionInterface
{
}

/**
 * Interface that must be implemented by classes that provide their own mechanism
 * to parse and handle connection parameters.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ConnectionParametersInterface
{
    /**
     * Checks if the specified parameters is set.
     *
     * @param string $property Name of the property.
     * @return Boolean
     */
    public function __isset($parameter);

    /**
     * Returns the value of the specified parameter.
     *
     * @param string $parameter Name of the parameter.
     * @return mixed
     */
    public function __get($parameter);

    /**
     * Returns an array representation of the connection parameters.
     *
     * @return array
     */
    public function toArray();
}

/**
 * Defines a group of Redis servers in a master/slave replication configuration.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ReplicationConnectionInterface extends AggregatedConnectionInterface
{
    /**
     * Switches the internal connection object being used.
     *
     * @param string $connection Alias of a connection
     */
    public function switchTo($connection);

    /**
     * Retrieves the connection object currently being used.
     *
     * @return SingleConnectionInterface
     */
    public function getCurrent();

    /**
     * Retrieves the connection object to the master Redis server.
     *
     * @return SingleConnectionInterface
     */
    public function getMaster();

    /**
     * Retrieves a list of connection objects to slaves Redis servers.
     *
     * @return SingleConnectionInterface
     */
    public function getSlaves();
}

/**
 * Interface that must be implemented by classes that provide their own mechanism
 * to create and initialize new instances of Predis\Connection\SingleConnectionInterface.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ConnectionFactoryInterface
{
    /**
     * Defines or overrides the connection class identified by a scheme prefix.
     *
     * @param string $scheme URI scheme identifying the connection class.
     * @param mixed $initializer FQN of a connection class or a callable object for lazy initialization.
     */
    public function define($scheme, $initializer);

    /**
     * Undefines the connection identified by a scheme prefix.
     *
     * @param string $scheme Parameters for the connection.
     */
    public function undefine($scheme);

    /**
     * Creates a new connection object.
     *
     * @param mixed $parameters Parameters for the connection.
     * @return SingleConnectionInterface
     */
    public function create($parameters);

    /**
     * Prepares an aggregation of connection objects.
     *
     * @param AggregatedConnectionInterface $cluster Instance of an aggregated connection class.
     * @param array $parameters List of parameters for each connection object.
     * @return AggregatedConnectionInterface
     */
    public function createAggregated(AggregatedConnectionInterface $cluster, Array $parameters);
}

/**
 * Defines a connection object used to communicate with a single Redis server
 * that leverages an external protocol processor to handle pluggable protocol
 * handlers.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ComposableConnectionInterface extends SingleConnectionInterface
{
    /**
     * Sets the protocol processor used by the connection.
     *
     * @param ProtocolInterface $protocol Protocol processor.
     */
    public function setProtocol(ProtocolInterface $protocol);

    /**
     * Gets the protocol processor used by the connection.
     */
    public function getProtocol();

    /**
     * Writes a buffer that contains a serialized Redis command.
     *
     * @param string $buffer Serialized Redis command.
     */
    public function writeBytes($buffer);

    /**
     * Reads a specified number of bytes from the connection.
     *
     * @param string
     */
    public function readBytes($length);

    /**
     * Reads a line from the connection.
     *
     * @param string
     */
    public function readLine();
}

/**
 * Abstraction for Redis cluster (Redis v3.0).
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class RedisCluster implements ClusterConnectionInterface, \IteratorAggregate, \Countable
{
    private $pool;
    private $slots;
    private $slotsMap;
    private $slotsPerNode;
    private $strategy;
    private $connections;

    /**
     * @param ConnectionFactoryInterface $connections Connection factory object.
     */
    public function __construct(ConnectionFactoryInterface $connections = null)
    {
        $this->pool = array();
        $this->slots = array();
        $this->strategy = new RedisClusterHashStrategy();
        $this->connections = $connections ?: new ConnectionFactory();
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected()
    {
        foreach ($this->pool as $connection) {
            if ($connection->isConnected()) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        foreach ($this->pool as $connection) {
            $connection->connect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        foreach ($this->pool as $connection) {
            $connection->disconnect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function add(SingleConnectionInterface $connection)
    {
        $this->pool[(string) $connection] = $connection;
        unset(
            $this->slotsMap,
            $this->slotsPerNode
        );
    }

    /**
     * {@inheritdoc}
     */
    public function remove(SingleConnectionInterface $connection)
    {
        if (($id = array_search($connection, $this->pool, true)) !== false) {
            unset(
                $this->pool[$id],
                $this->slotsMap,
                $this->slotsPerNode
            );

            return true;
        }

        return false;
    }

    /**
     * Removes a connection instance using its alias or index.
     *
     * @param string $connectionId Alias or index of a connection.
     * @return Boolean Returns true if the connection was in the pool.
     */
    public function removeById($connectionId)
    {
        if (isset($this->pool[$connectionId])) {
            unset(
                $this->pool[$connectionId],
                $this->slotsMap,
                $this->slotsPerNode
            );

            return true;
        }

        return false;
    }

    /**
     * Builds the slots map for the cluster.
     *
     * @return array
     */
    public function buildSlotsMap()
    {
        $this->slotsMap = array();
        $this->slotsPerNode = (int) (16384 / count($this->pool));

        foreach ($this->pool as $connectionID => $connection) {
            $parameters = $connection->getParameters();

            if (!isset($parameters->slots)) {
                continue;
            }

            list($first, $last) = explode('-', $parameters->slots, 2);
            $this->setSlots($first, $last, $connectionID);
        }

        return $this->slotsMap;
    }

    /**
     * Returns the current slots map for the cluster.
     *
     * @return array
     */
    public function getSlotsMap()
    {
        if (!isset($this->slotsMap)) {
            $this->slotsMap = array();
        }

        return $this->slotsMap;
    }

    /**
     * Preassociate a connection to a set of slots to avoid runtime guessing.
     *
     * @todo Check type or existence of the specified connection.
     * @todo Cluster loses the slots assigned with this methods when adding / removing connections.
     *
     * @param int $first Initial slot.
     * @param int $last Last slot.
     * @param SingleConnectionInterface|string $connection ID or connection instance.
     */
    public function setSlots($first, $last, $connection)
    {
        if ($first < 0x0000 || $first > 0x3FFF || $last < 0x0000 || $last > 0x3FFF || $last < $first) {
            throw new \OutOfBoundsException("Invalid slot values for $connection: [$first-$last]");
        }

        $this->slotsMap = $this->getSlotsMap() + array_fill($first, $last - $first + 1, (string) $connection);
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection(CommandInterface $command)
    {
        $hash = $this->strategy->getHash($command);

        if (!isset($hash)) {
            throw new NotSupportedException("Cannot use {$command->getId()} with redis-cluster");
        }

        $slot = $hash & 0x3FFF;

        if (isset($this->slots[$slot])) {
            return $this->slots[$slot];
        }

        $this->slots[$slot] = $connection = $this->pool[$this->guessNode($slot)];

        return $connection;
    }

    /**
     * Returns the connection associated to the specified slot.
     *
     * @param int $slot Slot ID.
     * @return SingleConnectionInterface
     */
    public function getConnectionBySlot($slot)
    {
        if ($slot < 0x0000 || $slot > 0x3FFF) {
            throw new \OutOfBoundsException("Invalid slot value [$slot]");
        }

        if (isset($this->slots[$slot])) {
            return $this->slots[$slot];
        }

        return $this->pool[$this->guessNode($slot)];
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionById($connectionId)
    {
        return isset($this->pool[$connectionId]) ? $this->pool[$connectionId] : null;
    }

    /**
     * Tries guessing the correct node associated to the given slot using a precalculated
     * slots map or the same logic used by redis-trib to initialize a redis cluster.
     *
     * @param int $slot Slot ID.
     * @return string
     */
    protected function guessNode($slot)
    {
        if (!isset($this->slotsMap)) {
            $this->buildSlotsMap();
        }

        if (isset($this->slotsMap[$slot])) {
            return $this->slotsMap[$slot];
        }

        $index = min((int) ($slot / $this->slotsPerNode), count($this->pool) - 1);
        $nodes = array_keys($this->pool);

        return $nodes[$index];
    }

    /**
     * Handles -MOVED or -ASK replies by re-executing the command on the server
     * specified by the Redis reply.
     *
     * @param CommandInterface $command Command that generated the -MOVE or -ASK reply.
     * @param string $request Type of request (either 'MOVED' or 'ASK').
     * @param string $details Parameters of the MOVED/ASK request.
     * @return mixed
     */
    protected function onMoveRequest(CommandInterface $command, $request, $details)
    {
        list($slot, $host) = explode(' ', $details, 2);
        $connection = $this->getConnectionById($host);

        if (!isset($connection)) {
            $parameters = array('host' => null, 'port' => null);
            list($parameters['host'], $parameters['port']) = explode(':', $host, 2);
            $connection = $this->connections->create($parameters);
        }

        switch ($request) {
            case 'MOVED':
                $this->move($connection, $slot);
                return $this->executeCommand($command);

            case 'ASK':
                return $connection->executeCommand($command);

            default:
                throw new ClientException("Unexpected request type for a move request: $request");
        }
    }

    /**
     * Assign the connection instance to a new slot and adds it to the
     * pool if the connection was not already part of the pool.
     *
     * @param SingleConnectionInterface $connection Connection instance
     * @param int $slot Target slot.
     */
    protected function move(SingleConnectionInterface $connection, $slot)
    {
        $this->pool[(string) $connection] = $connection;
        $this->slots[(int) $slot] = $connection;
    }

    /**
     * Returns the underlying command hash strategy used to hash
     * commands by their keys.
     *
     * @return CommandHashStrategyInterface
     */
    public function getCommandHashStrategy()
    {
        return $this->strategy;
    }

    /**
     * {@inheritdoc}
     */
    public function count()
    {
        return count($this->pool);
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        return new \ArrayIterator(array_values($this->pool));
    }

    /**
     * Handles -ERR replies from Redis.
     *
     * @param CommandInterface $command Command that generated the -ERR reply.
     * @param ResponseErrorInterface $error Redis error reply object.
     * @return mixed
     */
    protected function handleServerError(CommandInterface $command, ResponseErrorInterface $error)
    {
        list($type, $details) = explode(' ', $error->getMessage(), 2);

        switch ($type) {
            case 'MOVED':
            case 'ASK':
                return $this->onMoveRequest($command, $type, $details);

            default:
                return $error;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $this->getConnection($command)->writeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        return $this->getConnection($command)->readResponse($command);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        $connection = $this->getConnection($command);
        $reply = $connection->executeCommand($command);

        if ($reply instanceof ResponseErrorInterface) {
            return $this->handleServerError($command, $reply);
        }

        return $reply;
    }
}

/**
 * This class implements a Predis connection that actually talks with Webdis
 * instead of connecting directly to Redis. It relies on the cURL extension to
 * communicate with the web server and the phpiredis extension to parse the
 * protocol of the replies returned in the http response bodies.
 *
 * Some features are not yet available or they simply cannot be implemented:
 *   - Pipelining commands.
 *   - Publish / Subscribe.
 *   - MULTI / EXEC transactions (not yet supported by Webdis).
 *
 * The connection parameters supported by this class are:
 *
 *  - scheme: must be 'http'.
 *  - host: hostname or IP address of the server.
 *  - port: TCP port of the server.
 *  - timeout: timeout to perform the connection.
 *  - user: username for authentication.
 *  - pass: password for authentication.
 *
 * @link http://webd.is
 * @link http://github.com/nicolasff/webdis
 * @link http://github.com/seppo0010/phpiredis
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class WebdisConnection implements SingleConnectionInterface
{
    const ERR_MSG_EXTENSION = 'The %s extension must be loaded in order to be able to use this connection class';

    private $parameters;
    private $resource;
    private $reader;

    /**
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     */
    public function __construct(ConnectionParametersInterface $parameters)
    {
        $this->checkExtensions();

        if ($parameters->scheme !== 'http') {
            throw new \InvalidArgumentException("Invalid scheme: {$parameters->scheme}");
        }

        $this->parameters = $parameters;
        $this->resource = $this->initializeCurl($parameters);
        $this->reader = $this->initializeReader($parameters);
    }

    /**
     * Frees the underlying cURL and protocol reader resources when PHP's
     * garbage collector kicks in.
     */
    public function __destruct()
    {
        curl_close($this->resource);
        phpiredis_reader_destroy($this->reader);
    }

    /**
     * Helper method used to throw on unsupported methods.
     */
    private function throwNotSupportedException($function)
    {
        $class = __CLASS__;
        throw new NotSupportedException("The method $class::$function() is not supported");
    }

    /**
     * Checks if the cURL and phpiredis extensions are loaded in PHP.
     */
    private function checkExtensions()
    {
        if (!function_exists('curl_init')) {
            throw new NotSupportedException(sprintf(self::ERR_MSG_EXTENSION, 'curl'));
        }

        if (!function_exists('phpiredis_reader_create')) {
            throw new NotSupportedException(sprintf(self::ERR_MSG_EXTENSION, 'phpiredis'));
        }
    }

    /**
     * Initializes cURL.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return resource
     */
    private function initializeCurl(ConnectionParametersInterface $parameters)
    {
        $options = array(
            CURLOPT_FAILONERROR => true,
            CURLOPT_CONNECTTIMEOUT_MS => $parameters->timeout * 1000,
            CURLOPT_URL => "{$parameters->scheme}://{$parameters->host}:{$parameters->port}",
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_POST => true,
            CURLOPT_WRITEFUNCTION => array($this, 'feedReader'),
        );

        if (isset($parameters->user, $parameters->pass)) {
            $options[CURLOPT_USERPWD] = "{$parameters->user}:{$parameters->pass}";
        }

        curl_setopt_array($resource = curl_init(), $options);

        return $resource;
    }

    /**
     * Initializes phpiredis' protocol reader.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return resource
     */
    private function initializeReader(ConnectionParametersInterface $parameters)
    {
        $reader = phpiredis_reader_create();

        phpiredis_reader_set_status_handler($reader, $this->getStatusHandler());
        phpiredis_reader_set_error_handler($reader, $this->getErrorHandler());

        return $reader;
    }

    /**
     * Gets the handler used by the protocol reader to handle status replies.
     *
     * @return \Closure
     */
    protected function getStatusHandler()
    {
        return function ($payload) {
            return $payload === 'OK' ? true : $payload;
        };
    }

    /**
     * Gets the handler used by the protocol reader to handle Redis errors.
     *
     * @return \Closure
     */
    protected function getErrorHandler()
    {
        return function ($errorMessage) {
            return new ResponseError($errorMessage);
        };
    }

    /**
     * Feeds phpredis' reader resource with the data read from the network.
     *
     * @param resource $resource Reader resource.
     * @param string $buffer Buffer with the reply read from the network.
     * @return int
     */
    protected function feedReader($resource, $buffer)
    {
        phpiredis_reader_feed($this->reader, $buffer);

        return strlen($buffer);
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        // NOOP
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        // NOOP
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected()
    {
        return true;
    }

    /**
     * Checks if the specified command is supported by this connection class.
     *
     * @param CommandInterface $command The instance of a Redis command.
     * @return string
     */
    protected function getCommandId(CommandInterface $command)
    {
        switch (($commandId = $command->getId())) {
            case 'AUTH':
            case 'SELECT':
            case 'MULTI':
            case 'EXEC':
            case 'WATCH':
            case 'UNWATCH':
            case 'DISCARD':
            case 'MONITOR':
                throw new NotSupportedException("Disabled command: {$command->getId()}");

            default:
                return $commandId;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $this->throwNotSupportedException(__FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        $this->throwNotSupportedException(__FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        $resource = $this->resource;
        $commandId = $this->getCommandId($command);

        if ($arguments = $command->getArguments()) {
            $arguments = implode('/', array_map('urlencode', $arguments));
            $serializedCommand = "$commandId/$arguments.raw";
        } else {
            $serializedCommand = "$commandId.raw";
        }

        curl_setopt($resource, CURLOPT_POSTFIELDS, $serializedCommand);

        if (curl_exec($resource) === false) {
            $error = curl_error($resource);
            $errno = curl_errno($resource);
            throw new ConnectionException($this, trim($error), $errno);
        }

        if (phpiredis_reader_get_state($this->reader) !== PHPIREDIS_READER_STATE_COMPLETE) {
            throw new ProtocolException($this, phpiredis_reader_get_error($this->reader));
        }

        return phpiredis_reader_get_reply($this->reader);
    }

    /**
     * {@inheritdoc}
     */
    public function getResource()
    {
        return $this->resource;
    }

    /**
     * {@inheritdoc}
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function pushInitCommand(CommandInterface $command)
    {
        $this->throwNotSupportedException(__FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        $this->throwNotSupportedException(__FUNCTION__);
    }

    /**
     * {@inheritdoc}
     */
    public function __toString()
    {
        return "{$this->parameters->host}:{$this->parameters->port}";
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array('parameters');
    }

    /**
     * {@inheritdoc}
     */
    public function __wakeup()
    {
        $this->checkExtensions();
        $parameters = $this->getParameters();

        $this->resource = $this->initializeCurl($parameters);
        $this->reader = $this->initializeReader($parameters);
    }
}

/**
 * Abstraction for a cluster of aggregated connections to various Redis servers
 * implementing client-side sharding based on pluggable distribution strategies.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 * @todo Add the ability to remove connections from pool.
 */
class PredisCluster implements ClusterConnectionInterface, \IteratorAggregate, \Countable
{
    private $pool;
    private $strategy;
    private $distributor;

    /**
     * @param DistributionStrategyInterface $distributor Distribution strategy used by the cluster.
     */
    public function __construct(DistributionStrategyInterface $distributor = null)
    {
        $distributor = $distributor ?: new HashRing();

        $this->pool = array();
        $this->strategy = new PredisClusterHashStrategy($distributor->getHashGenerator());
        $this->distributor = $distributor;
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected()
    {
        foreach ($this->pool as $connection) {
            if ($connection->isConnected()) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        foreach ($this->pool as $connection) {
            $connection->connect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        foreach ($this->pool as $connection) {
            $connection->disconnect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function add(SingleConnectionInterface $connection)
    {
        $parameters = $connection->getParameters();

        if (isset($parameters->alias)) {
            $this->pool[$parameters->alias] = $connection;
        } else {
            $this->pool[] = $connection;
        }

        $weight = isset($parameters->weight) ? $parameters->weight : null;
        $this->distributor->add($connection, $weight);
    }

    /**
     * {@inheritdoc}
     */
    public function remove(SingleConnectionInterface $connection)
    {
        if (($id = array_search($connection, $this->pool, true)) !== false) {
            unset($this->pool[$id]);
            $this->distributor->remove($connection);

            return true;
        }

        return false;
    }

    /**
     * Removes a connection instance using its alias or index.
     *
     * @param string $connectionId Alias or index of a connection.
     * @return Boolean Returns true if the connection was in the pool.
     */
    public function removeById($connectionId)
    {
        if ($connection = $this->getConnectionById($connectionId)) {
            return $this->remove($connection);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection(CommandInterface $command)
    {
        $hash = $this->strategy->getHash($command);

        if (!isset($hash)) {
            throw new NotSupportedException("Cannot use {$command->getId()} with a cluster of connections");
        }

        $node = $this->distributor->get($hash);

        return $node;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionById($connectionId)
    {
        return isset($this->pool[$connectionId]) ? $this->pool[$connectionId] : null;
    }

    /**
     * Retrieves a connection instance from the cluster using a key.
     *
     * @param string $key Key of a Redis value.
     * @return SingleConnectionInterface
     */
    public function getConnectionByKey($key)
    {
        $hash = $this->strategy->getKeyHash($key);
        $node = $this->distributor->get($hash);

        return $node;
    }

    /**
     * Returns the underlying command hash strategy used to hash
     * commands by their keys.
     *
     * @return CommandHashStrategyInterface
     */
    public function getCommandHashStrategy()
    {
        return $this->strategy;
    }

    /**
     * {@inheritdoc}
     */
    public function count()
    {
        return count($this->pool);
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->pool);
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $this->getConnection($command)->writeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        return $this->getConnection($command)->readResponse($command);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        return $this->getConnection($command)->executeCommand($command);
    }

    /**
     * Executes the specified Redis command on all the nodes of a cluster.
     *
     * @param CommandInterface $command A Redis command.
     * @return array
     */
    public function executeCommandOnNodes(CommandInterface $command)
    {
        $replies = array();

        foreach ($this->pool as $connection) {
            $replies[] = $connection->executeCommand($command);
        }

        return $replies;
    }
}

/**
 * Handles parsing and validation of connection parameters.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionParameters implements ConnectionParametersInterface
{
    private $parameters;

    private static $defaults = array(
        'scheme' => 'tcp',
        'host' => '127.0.0.1',
        'port' => 6379,
        'timeout' => 5.0,
    );

    /**
     * @param string|array Connection parameters in the form of an URI string or a named array.
     */
    public function __construct($parameters = array())
    {
        if (!is_array($parameters)) {
            $parameters = self::parseURI($parameters);
        }

        $this->parameters = $this->filter($parameters) + $this->getDefaults();
    }

    /**
     * Returns some default parameters with their values.
     *
     * @return array
     */
    protected function getDefaults()
    {
        return self::$defaults;
    }

    /**
     * Returns cast functions for user-supplied parameter values.
     *
     * @return array
     */
    protected function getValueCasters()
    {
        return array(
            'port' => 'self::castInteger',
            'async_connect' => 'self::castBoolean',
            'persistent' => 'self::castBoolean',
            'timeout' => 'self::castFloat',
            'read_write_timeout' => 'self::castFloat',
            'iterable_multibulk' => 'self::castBoolean',
        );
    }

    /**
     * Validates value as boolean.
     *
     * @param mixed $value Input value.
     * @return boolean
     */
    private static function castBoolean($value)
    {
        return (bool) $value;
    }

    /**
     * Validates value as float.
     *
     * @param mixed $value Input value.
     * @return float
     */
    private static function castFloat($value)
    {
        return (float) $value;
    }

    /**
     * Validates value as integer.
     *
     * @param mixed $value Input value.
     * @return int
     */
    private static function castInteger($value)
    {
        return (int) $value;
    }

    /**
     * Parses an URI string and returns an array of connection parameters.
     *
     * @param string $uri Connection string.
     * @return array
     */
    public static function parseURI($uri)
    {
        if (stripos($uri, 'unix') === 0) {
            // Hack to support URIs for UNIX sockets with minimal effort.
            $uri = str_ireplace('unix:///', 'unix://localhost/', $uri);
        }

        if (!($parsed = @parse_url($uri)) || !isset($parsed['host'])) {
            throw new ClientException("Invalid URI: $uri");
        }

        if (isset($parsed['query'])) {
            foreach (explode('&', $parsed['query']) as $kv) {
                @list($k, $v) = explode('=', $kv);
                $parsed[$k] = $v;
            }

            unset($parsed['query']);
        }

        return $parsed;
    }

    /**
     * Validates and converts each value of the connection parameters array.
     *
     * @param array $parameters Connection parameters.
     * @return array
     */
    private function filter(Array $parameters)
    {
        if ($parameters) {
            $casters = array_intersect_key($this->getValueCasters(), $parameters);

            foreach ($casters as $parameter => $caster) {
                $parameters[$parameter] = call_user_func($caster, $parameters[$parameter]);
            }
        }

        return $parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function __get($parameter)
    {
        if (isset($this->{$parameter})) {
            return $this->parameters[$parameter];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function __isset($parameter)
    {
        return isset($this->parameters[$parameter]);
    }

    /**
     * {@inheritdoc}
     */
    public function toArray()
    {
        return $this->parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array('parameters');
    }
}

/**
 * Exception class that identifies connection-related errors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionException extends CommunicationException
{
}

/**
 * Connection abstraction to Redis servers based on PHP's stream that uses an
 * external protocol processor defining the protocol used for the communication.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ComposableStreamConnection extends StreamConnection implements ComposableConnectionInterface
{
    private $protocol;

    /**
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @param ProtocolInterface $protocol A protocol processor.
     */
    public function __construct(ConnectionParametersInterface $parameters, ProtocolInterface $protocol = null)
    {
        $this->parameters = $this->checkParameters($parameters);
        $this->protocol = $protocol ?: new TextProtocol();
    }

    /**
     * {@inheritdoc}
     */
    public function setProtocol(ProtocolInterface $protocol)
    {
        if ($protocol === null) {
            throw new \InvalidArgumentException("The protocol instance cannot be a null value");
        }

        $this->protocol = $protocol;
    }

    /**
     * {@inheritdoc}
     */
    public function getProtocol()
    {
        return $this->protocol;
    }

    /**
     * {@inheritdoc}
     */
    public function writeBytes($buffer)
    {
        parent::writeBytes($buffer);
    }

    /**
     * {@inheritdoc}
     */
    public function readBytes($length)
    {
        if ($length <= 0) {
            throw new \InvalidArgumentException('Length parameter must be greater than 0');
        }

        $value  = '';
        $socket = $this->getResource();

        do {
            $chunk = fread($socket, $length);

            if ($chunk === false || $chunk === '') {
                $this->onConnectionError('Error while reading bytes from the server');
            }

            $value .= $chunk;
        } while (($length -= strlen($chunk)) > 0);

        return $value;
    }

    /**
     * {@inheritdoc}
     */
    public function readLine()
    {
        $value  = '';
        $socket = $this->getResource();

        do {
            $chunk = fgets($socket);

            if ($chunk === false || $chunk === '') {
                $this->onConnectionError('Error while reading line from the server');
            }

            $value .= $chunk;
        } while (substr($value, -2) !== "\r\n");

        return substr($value, 0, -2);
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $this->protocol->write($this, $command);
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        return $this->protocol->read($this);
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array_diff(array_merge(parent::__sleep(), array('protocol')), array('mbiterable'));
    }
}

/**
 * Provides a default factory for Redis connections that maps URI schemes
 * to connection classes implementing Predis\Connection\SingleConnectionInterface.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ConnectionFactory implements ConnectionFactoryInterface
{
    protected $schemes;
    protected $profile;

    /**
     * Initializes a new instance of the default connection factory class used by Predis.
     *
     * @param ServerProfileInterface $profile Server profile used to initialize new connections.
     */
    public function __construct(ServerProfileInterface $profile = null)
    {
        $this->schemes = $this->getDefaultSchemes();
        $this->profile = $profile;
    }

    /**
     * Returns a named array that maps URI schemes to connection classes.
     *
     * @return array Map of URI schemes and connection classes.
     */
    protected function getDefaultSchemes()
    {
        return array(
            'tcp'  => 'Predis\Connection\StreamConnection',
            'unix' => 'Predis\Connection\StreamConnection',
            'http' => 'Predis\Connection\WebdisConnection',
        );
    }

    /**
     * Checks if the provided argument represents a valid connection class
     * implementing Predis\Connection\SingleConnectionInterface. Optionally,
     * callable objects are used for lazy initialization of connection objects.
     *
     * @param mixed $initializer FQN of a connection class or a callable for lazy initialization.
     * @return mixed
     */
    protected function checkInitializer($initializer)
    {
        if (is_callable($initializer)) {
            return $initializer;
        }

        $initializerReflection = new \ReflectionClass($initializer);

        if (!$initializerReflection->isSubclassOf('Predis\Connection\SingleConnectionInterface')) {
            throw new \InvalidArgumentException(
                'A connection initializer must be a valid connection class or a callable object'
            );
        }

        return $initializer;
    }

    /**
     * {@inheritdoc}
     */
    public function define($scheme, $initializer)
    {
        $this->schemes[$scheme] = $this->checkInitializer($initializer);
    }

    /**
     * {@inheritdoc}
     */
    public function undefine($scheme)
    {
        unset($this->schemes[$scheme]);
    }

    /**
     * {@inheritdoc}
     */
    public function create($parameters)
    {
        if (!$parameters instanceof ConnectionParametersInterface) {
            $parameters = new ConnectionParameters($parameters ?: array());
        }

        $scheme = $parameters->scheme;

        if (!isset($this->schemes[$scheme])) {
            throw new \InvalidArgumentException("Unknown connection scheme: $scheme");
        }

        $initializer = $this->schemes[$scheme];

        if (is_callable($initializer)) {
            $connection = call_user_func($initializer, $parameters, $this);
        } else {
            $connection = new $initializer($parameters);
            $this->prepareConnection($connection);
        }

        if (!$connection instanceof SingleConnectionInterface) {
            throw new \InvalidArgumentException(
                'Objects returned by connection initializers must implement ' .
                'Predis\Connection\SingleConnectionInterface'
            );
        }

        return $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function createAggregated(AggregatedConnectionInterface $connection, Array $parameters)
    {
        foreach ($parameters as $node) {
            $connection->add($node instanceof SingleConnectionInterface ? $node : $this->create($node));
        }

        return $connection;
    }

    /**
     * Prepares a connection object after its initialization.
     *
     * @param SingleConnectionInterface $connection Instance of a connection object.
     */
    protected function prepareConnection(SingleConnectionInterface $connection)
    {
        if (isset($this->profile)) {
            $parameters = $connection->getParameters();

            if (isset($parameters->password)) {
                $command = $this->profile->createCommand('auth', array($parameters->password));
                $connection->pushInitCommand($command);
            }

            if (isset($parameters->database)) {
                $command = $this->profile->createCommand('select', array($parameters->database));
                $connection->pushInitCommand($command);
            }
        }
    }

    /**
     * Sets the server profile used to create initialization commands for connections.
     *
     * @param ServerProfileInterface $profile Server profile instance.
     */
    public function setProfile(ServerProfileInterface $profile)
    {
        $this->profile = $profile;
    }

    /**
     * Returns the server profile used to create initialization commands for connections.
     *
     * @return ServerProfileInterface
     */
    public function getProfile()
    {
        return $this->profile;
    }
}

/**
 * Aggregated connection class used by to handle replication with a
 * group of servers in a master/slave configuration.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MasterSlaveReplication implements ReplicationConnectionInterface
{
    protected $strategy;
    protected $master;
    protected $slaves;
    protected $current;

    /**
     *
     */
    public function __construct(ReplicationStrategy $strategy = null)
    {
        $this->slaves = array();
        $this->strategy = $strategy ?: new ReplicationStrategy();
    }

    /**
     * Checks if one master and at least one slave have been defined.
     */
    protected function check()
    {
        if (!isset($this->master) || !$this->slaves) {
            throw new \RuntimeException('Replication needs a master and at least one slave.');
        }
    }

    /**
     * Resets the connection state.
     */
    protected function reset()
    {
        $this->current = null;
    }

    /**
     * {@inheritdoc}
     */
    public function add(SingleConnectionInterface $connection)
    {
        $alias = $connection->getParameters()->alias;

        if ($alias === 'master') {
            $this->master = $connection;
        } else {
            $this->slaves[$alias ?: count($this->slaves)] = $connection;
        }

        $this->reset();
    }

    /**
     * {@inheritdoc}
     */
    public function remove(SingleConnectionInterface $connection)
    {
        if ($connection->getParameters()->alias === 'master') {
            $this->master = null;
            $this->reset();

            return true;
        } else {
            if (($id = array_search($connection, $this->slaves, true)) !== false) {
                unset($this->slaves[$id]);
                $this->reset();

                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection(CommandInterface $command)
    {
        if ($this->current === null) {
            $this->check();
            $this->current = $this->strategy->isReadOperation($command) ? $this->pickSlave() : $this->master;

            return $this->current;
        }

        if ($this->current === $this->master) {
            return $this->current;
        }

        if (!$this->strategy->isReadOperation($command)) {
            $this->current = $this->master;
        }

        return $this->current;
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionById($connectionId)
    {
        if ($connectionId === 'master') {
            return $this->master;
        }

        if (isset($this->slaves[$connectionId])) {
            return $this->slaves[$connectionId];
        }

        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function switchTo($connection)
    {
        $this->check();

        if (!$connection instanceof SingleConnectionInterface) {
            $connection = $this->getConnectionById($connection);
        }
        if ($connection !== $this->master && !in_array($connection, $this->slaves, true)) {
            throw new \InvalidArgumentException('The specified connection is not valid.');
        }

        $this->current = $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent()
    {
        return $this->current;
    }

    /**
     * {@inheritdoc}
     */
    public function getMaster()
    {
        return $this->master;
    }

    /**
     * {@inheritdoc}
     */
    public function getSlaves()
    {
        return array_values($this->slaves);
    }

    /**
     * Returns the underlying replication strategy.
     *
     * @return ReplicationStrategy
     */
    public function getReplicationStrategy()
    {
        return $this->strategy;
    }

    /**
     * Returns a random slave.
     *
     * @return SingleConnectionInterface
     */
    protected function pickSlave()
    {
        return $this->slaves[array_rand($this->slaves)];
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected()
    {
        return $this->current ? $this->current->isConnected() : false;
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        if ($this->current === null) {
            $this->check();
            $this->current = $this->pickSlave();
        }

        $this->current->connect();
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        if ($this->master) {
            $this->master->disconnect();
        }

        foreach ($this->slaves as $connection) {
            $connection->disconnect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $this->getConnection($command)->writeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    public function readResponse(CommandInterface $command)
    {
        return $this->getConnection($command)->readResponse($command);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        return $this->getConnection($command)->executeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array('master', 'slaves', 'strategy');
    }
}

/**
 * This class provides the implementation of a Predis connection that uses the
 * PHP socket extension for network communication and wraps the phpiredis C
 * extension (PHP bindings for hiredis) to parse the Redis protocol. Everything
 * is highly experimental (even the very same phpiredis since it is quite new),
 * so use it at your own risk.
 *
 * This class is mainly intended to provide an optional low-overhead alternative
 * for processing replies from Redis compared to the standard pure-PHP classes.
 * Differences in speed when dealing with short inline replies are practically
 * nonexistent, the actual speed boost is for long multibulk replies when this
 * protocol processor can parse and return replies very fast.
 *
 * For instructions on how to build and install the phpiredis extension, please
 * consult the repository of the project.
 *
 * The connection parameters supported by this class are:
 *
 *  - scheme: it can be either 'tcp' or 'unix'.
 *  - host: hostname or IP address of the server.
 *  - port: TCP port of the server.
 *  - timeout: timeout to perform the connection.
 *  - read_write_timeout: timeout of read / write operations.
 *
 * @link http://github.com/nrk/phpiredis
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PhpiredisConnection extends AbstractConnection
{
    const ERR_MSG_EXTENSION = 'The %s extension must be loaded in order to be able to use this connection class';

    private $reader;

    /**
     * {@inheritdoc}
     */
    public function __construct(ConnectionParametersInterface $parameters)
    {
        $this->checkExtensions();
        $this->initializeReader();

        parent::__construct($parameters);
    }

    /**
     * Disconnects from the server and destroys the underlying resource and the
     * protocol reader resource when PHP's garbage collector kicks in.
     */
    public function __destruct()
    {
        phpiredis_reader_destroy($this->reader);

        parent::__destruct();
    }

    /**
     * Checks if the socket and phpiredis extensions are loaded in PHP.
     */
    private function checkExtensions()
    {
        if (!function_exists('socket_create')) {
            throw new NotSupportedException(sprintf(self::ERR_MSG_EXTENSION, 'socket'));
        }
        if (!function_exists('phpiredis_reader_create')) {
            throw new NotSupportedException(sprintf(self::ERR_MSG_EXTENSION, 'phpiredis'));
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function checkParameters(ConnectionParametersInterface $parameters)
    {
        if (isset($parameters->iterable_multibulk)) {
            $this->onInvalidOption('iterable_multibulk', $parameters);
        }
        if (isset($parameters->persistent)) {
            $this->onInvalidOption('persistent', $parameters);
        }

        return parent::checkParameters($parameters);
    }

    /**
     * Initializes the protocol reader resource.
     */
    private function initializeReader()
    {
        $reader = phpiredis_reader_create();

        phpiredis_reader_set_status_handler($reader, $this->getStatusHandler());
        phpiredis_reader_set_error_handler($reader, $this->getErrorHandler());

        $this->reader = $reader;
    }

    /**
     * Gets the handler used by the protocol reader to handle status replies.
     *
     * @return \Closure
     */
    private function getStatusHandler()
    {
        return function ($payload) {
            switch ($payload) {
                case 'OK':
                    return true;

                case 'QUEUED':
                    return new ResponseQueued();

                default:
                    return $payload;
            }
        };
    }

    /**
     * Gets the handler used by the protocol reader to handle Redis errors.
     *
     * @param Boolean $throw_errors Specify if Redis errors throw exceptions.
     * @return \Closure
     */
    private function getErrorHandler()
    {
        return function ($errorMessage) {
            return new ResponseError($errorMessage);
        };
    }

    /**
     * Helper method used to throw exceptions on socket errors.
     */
    private function emitSocketError()
    {
        $errno  = socket_last_error();
        $errstr = socket_strerror($errno);

        $this->disconnect();

        $this->onConnectionError(trim($errstr), $errno);
    }

    /**
     * {@inheritdoc}
     */
    protected function createResource()
    {
        $parameters = $this->parameters;

        $isUnix = $this->parameters->scheme === 'unix';
        $domain = $isUnix ? AF_UNIX : AF_INET;
        $protocol = $isUnix ? 0 : SOL_TCP;

        $socket = @call_user_func('socket_create', $domain, SOCK_STREAM, $protocol);
        if (!is_resource($socket)) {
            $this->emitSocketError();
        }

        $this->setSocketOptions($socket, $parameters);

        return $socket;
    }

    /**
     * Sets options on the socket resource from the connection parameters.
     *
     * @param resource $socket Socket resource.
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     */
    private function setSocketOptions($socket, ConnectionParametersInterface $parameters)
    {
        if ($parameters->scheme !== 'tcp') {
            return;
        }

        if (!socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1)) {
            $this->emitSocketError();
        }

        if (!socket_set_option($socket, SOL_SOCKET, SO_REUSEADDR, 1)) {
            $this->emitSocketError();
        }

        if (isset($parameters->read_write_timeout)) {
            $rwtimeout = $parameters->read_write_timeout;
            $timeoutSec = floor($rwtimeout);
            $timeoutUsec = ($rwtimeout - $timeoutSec) * 1000000;

            $timeout = array(
                'sec' => $timeoutSec,
                'usec' => $timeoutUsec,
            );

            if (!socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, $timeout)) {
                $this->emitSocketError();
            }

            if (!socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, $timeout)) {
                $this->emitSocketError();
            }
        }
    }

    /**
     * Gets the address from the connection parameters.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return string
     */
    private function getAddress(ConnectionParametersInterface $parameters)
    {
        if ($parameters->scheme === 'unix') {
            return $parameters->path;
        }

        $host = $parameters->host;

        if (ip2long($host) === false) {
            if (($addresses = gethostbynamel($host)) === false) {
                $this->onConnectionError("Cannot resolve the address of $host");
            }
            return $addresses[array_rand($addresses)];
        }

        return $host;
    }

    /**
     * Opens the actual connection to the server with a timeout.
     *
     * @param ConnectionParametersInterface $parameters Parameters used to initialize the connection.
     * @return string
     */
    private function connectWithTimeout(ConnectionParametersInterface $parameters)
    {
        $host = self::getAddress($parameters);
        $socket = $this->getResource();

        socket_set_nonblock($socket);

        if (@socket_connect($socket, $host, $parameters->port) === false) {
            $error = socket_last_error();
            if ($error != SOCKET_EINPROGRESS && $error != SOCKET_EALREADY) {
                $this->emitSocketError();
            }
        }

        socket_set_block($socket);

        $null = null;
        $selectable = array($socket);

        $timeout = $parameters->timeout;
        $timeoutSecs = floor($timeout);
        $timeoutUSecs = ($timeout - $timeoutSecs) * 1000000;

        $selected = socket_select($selectable, $selectable, $null, $timeoutSecs, $timeoutUSecs);

        if ($selected === 2) {
            $this->onConnectionError('Connection refused', SOCKET_ECONNREFUSED);
        }
        if ($selected === 0) {
            $this->onConnectionError('Connection timed out', SOCKET_ETIMEDOUT);
        }
        if ($selected === false) {
            $this->emitSocketError();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        parent::connect();

        $this->connectWithTimeout($this->parameters);

        if ($this->initCmds) {
            $this->sendInitializationCommands();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        if ($this->isConnected()) {
            socket_close($this->getResource());
            parent::disconnect();
        }
    }

    /**
     * Sends the initialization commands to Redis when the connection is opened.
     */
    private function sendInitializationCommands()
    {
        foreach ($this->initCmds as $command) {
            $this->writeCommand($command);
        }
        foreach ($this->initCmds as $command) {
            $this->readResponse($command);
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function write($buffer)
    {
        $socket = $this->getResource();

        while (($length = strlen($buffer)) > 0) {
            $written = socket_write($socket, $buffer, $length);

            if ($length === $written) {
                return;
            }
            if ($written === false) {
                $this->onConnectionError('Error while writing bytes to the server');
            }

            $buffer = substr($buffer, $written);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        $socket = $this->getResource();
        $reader = $this->reader;

        while (($state = phpiredis_reader_get_state($reader)) === PHPIREDIS_READER_STATE_INCOMPLETE) {
            if (@socket_recv($socket, $buffer, 4096, 0) === false || $buffer === '') {
                $this->emitSocketError();
            }

            phpiredis_reader_feed($reader, $buffer);
        }

        if ($state === PHPIREDIS_READER_STATE_COMPLETE) {
            return phpiredis_reader_get_reply($reader);
        } else {
            $this->onProtocolError(phpiredis_reader_get_error($reader));
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $cmdargs = $command->getArguments();
        array_unshift($cmdargs, $command->getId());
        $this->write(phpiredis_format_command($cmdargs));
    }

    /**
     * {@inheritdoc}
     */
    public function __wakeup()
    {
        $this->checkExtensions();
        $this->initializeReader();
    }
}

/**
 * This class provides the implementation of a Predis connection that uses PHP's
 * streams for network communication and wraps the phpiredis C extension (PHP
 * bindings for hiredis) to parse and serialize the Redis protocol. Everything
 * is highly experimental (even the very same phpiredis since it is quite new),
 * so use it at your own risk.
 *
 * This class is mainly intended to provide an optional low-overhead alternative
 * for processing replies from Redis compared to the standard pure-PHP classes.
 * Differences in speed when dealing with short inline replies are practically
 * nonexistent, the actual speed boost is for long multibulk replies when this
 * protocol processor can parse and return replies very fast.
 *
 * For instructions on how to build and install the phpiredis extension, please
 * consult the repository of the project.
 *
 * The connection parameters supported by this class are:
 *
 *  - scheme: it can be either 'tcp' or 'unix'.
 *  - host: hostname or IP address of the server.
 *  - port: TCP port of the server.
 *  - timeout: timeout to perform the connection.
 *  - read_write_timeout: timeout of read / write operations.
 *  - async_connect: performs the connection asynchronously.
 *  - tcp_nodelay: enables or disables Nagle's algorithm for coalescing.
 *  - persistent: the connection is left intact after a GC collection.
 *
 * @link https://github.com/nrk/phpiredis
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PhpiredisStreamConnection extends StreamConnection
{
    private $reader;

    /**
     * {@inheritdoc}
     */
    public function __construct(ConnectionParametersInterface $parameters)
    {
        $this->checkExtensions();
        $this->initializeReader();

        parent::__construct($parameters);
    }

    /**
     * {@inheritdoc}
     */
    public function __destruct()
    {
        phpiredis_reader_destroy($this->reader);

        parent::__destruct();
    }

    /**
     * Checks if the phpiredis extension is loaded in PHP.
     */
    protected function checkExtensions()
    {
        if (!function_exists('phpiredis_reader_create')) {
            throw new NotSupportedException(
                'The phpiredis extension must be loaded in order to be able to use this connection class'
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function checkParameters(ConnectionParametersInterface $parameters)
    {
        if (isset($parameters->iterable_multibulk)) {
            $this->onInvalidOption('iterable_multibulk', $parameters);
        }

        return parent::checkParameters($parameters);
    }

    /**
     * Initializes the protocol reader resource.
     */
    protected function initializeReader()
    {
        $reader = phpiredis_reader_create();

        phpiredis_reader_set_status_handler($reader, $this->getStatusHandler());
        phpiredis_reader_set_error_handler($reader, $this->getErrorHandler());

        $this->reader = $reader;
    }

    /**
     * Gets the handler used by the protocol reader to handle status replies.
     *
     * @return \Closure
     */
    protected function getStatusHandler()
    {
        return function ($payload) {
            switch ($payload) {
                case 'OK':
                    return true;

                case 'QUEUED':
                    return new ResponseQueued();

                default:
                    return $payload;
            }
        };
    }

    /**
     * Gets the handler used by the protocol reader to handle Redis errors.
     *
     * @param Boolean $throw_errors Specify if Redis errors throw exceptions.
     * @return \Closure
     */
    protected function getErrorHandler()
    {
        return function ($errorMessage) {
            return new ResponseError($errorMessage);
        };
    }

    /**
     * {@inheritdoc}
     */
    public function read()
    {
        $socket = $this->getResource();
        $reader = $this->reader;

        while (PHPIREDIS_READER_STATE_INCOMPLETE === $state = phpiredis_reader_get_state($reader)) {
            $buffer = fread($socket, 4096);

            if ($buffer === false || $buffer === '') {
                $this->onConnectionError('Error while reading bytes from the server');
                return;
            }

            phpiredis_reader_feed($reader, $buffer);
        }

        if ($state === PHPIREDIS_READER_STATE_COMPLETE) {
            return phpiredis_reader_get_reply($reader);
        } else {
            $this->onProtocolError(phpiredis_reader_get_error($reader));
        }
    }

    /**
     * {@inheritdoc}
     */
    public function writeCommand(CommandInterface $command)
    {
        $cmdargs = $command->getArguments();
        array_unshift($cmdargs, $command->getId());
        $this->writeBytes(phpiredis_format_command($cmdargs));
    }

    /**
     * {@inheritdoc}
     */
    public function __sleep()
    {
        return array_diff(parent::__sleep(), array('mbiterable'));
    }

    /**
     * {@inheritdoc}
     */
    public function __wakeup()
    {
        $this->checkExtensions();
        $this->initializeReader();
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis;

use Predis\Command\CommandInterface;
use Predis\Command\ScriptedCommand;
use Predis\Connection\AggregatedConnectionInterface;
use Predis\Connection\ConnectionInterface;
use Predis\Connection\ConnectionFactoryInterface;
use Predis\Monitor\MonitorContext;
use Predis\Option\ClientOptions;
use Predis\Option\ClientOptionsInterface;
use Predis\Pipeline\PipelineContext;
use Predis\Profile\ServerProfile;
use Predis\PubSub\PubSubContext;
use Predis\Transaction\MultiExecContext;
use Predis\Profile\ServerProfileInterface;
use Predis\Connection\SingleConnectionInterface;

/**
 * Represents a complex reply object from Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ResponseObjectInterface
{
}

/**
 * Base exception class for Predis-related errors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class PredisException extends \Exception
{
}

/**
 * Defines the interface of a basic client object or abstraction that
 * can send commands to Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface BasicClientInterface
{
    /**
     * Executes the specified Redis command.
     *
     * @param CommandInterface $command A Redis command.
     * @return mixed
     */
    public function executeCommand(CommandInterface $command);
}

/**
 * Base exception class for network-related errors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class CommunicationException extends PredisException
{
    private $connection;

    /**
     * @param SingleConnectionInterface $connection Connection that generated the exception.
     * @param string $message Error message.
     * @param int $code Error code.
     * @param \Exception $innerException Inner exception for wrapping the original error.
     */
    public function __construct(
        SingleConnectionInterface $connection, $message = null, $code = null, \Exception $innerException = null
    ) {
        parent::__construct($message, $code, $innerException);
        $this->connection = $connection;
    }

    /**
     * Gets the connection that generated the exception.
     *
     * @return SingleConnectionInterface
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Indicates if the receiver should reset the underlying connection.
     *
     * @return Boolean
     */
    public function shouldResetConnection()
    {
        return true;
    }

    /**
     * Offers a generic and reusable method to handle exceptions generated by
     * a connection object.
     *
     * @param CommunicationException $exception Exception.
     */
    public static function handle(CommunicationException $exception)
    {
        if ($exception->shouldResetConnection()) {
            $connection = $exception->getConnection();

            if ($connection->isConnected()) {
                $connection->disconnect();
            }
        }

        throw $exception;
    }
}

/**
 * Represents an error returned by Redis (replies identified by "-" in the
 * Redis response protocol) during the execution of an operation on the server.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ResponseErrorInterface extends ResponseObjectInterface
{
    /**
     * Returns the error message
     *
     * @return string
     */
    public function getMessage();

    /**
     * Returns the error type (e.g. ERR, ASK, MOVED)
     *
     * @return string
     */
    public function getErrorType();
}

/**
 * Defines the interface of a basic client object or abstraction that
 * can send commands to Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ExecutableContextInterface
{
    /**
     * Starts the execution of the context.
     *
     * @param mixed $callable Optional callback for execution.
     * @return array
     */
    public function execute($callable = null);
}

/**
 * Interface defining the most important parts needed to create an
 * high-level Redis client object that can interact with other
 * building blocks of Predis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ClientInterface extends BasicClientInterface
{
    /**
     * Returns the server profile used by the client.
     *
     * @return ServerProfileInterface
     */
    public function getProfile();

    /**
     * Returns the client options specified upon initialization.
     *
     * @return ClientOptionsInterface
     */
    public function getOptions();

    /**
     * Opens the connection to the server.
     */
    public function connect();

    /**
     * Disconnects from the server.
     */
    public function disconnect();

    /**
     * Returns the underlying connection instance.
     *
     * @return ConnectionInterface
     */
    public function getConnection();

    /**
     * Creates a new instance of the specified Redis command.
     *
     * @param string $method The name of a Redis command.
     * @param array $arguments The arguments for the command.
     * @return Command\CommandInterface
     */
    public function createCommand($method, $arguments = array());
}

/**
 * Exception class that identifies server-side Redis errors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerException extends PredisException implements ResponseErrorInterface
{
    /**
     * Gets the type of the error returned by Redis.
     *
     * @return string
     */
    public function getErrorType()
    {
        list($errorType, ) = explode(' ', $this->getMessage(), 2);

        return $errorType;
    }

    /**
     * Converts the exception to an instance of ResponseError.
     *
     * @return ResponseError
     */
    public function toResponseError()
    {
        return new ResponseError($this->getMessage());
    }
}

/**
 * Represents a +QUEUED response returned by Redis as a reply to each command
 * executed inside a MULTI/ EXEC transaction.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseQueued implements ResponseObjectInterface
{
    /**
     * Converts the object to its string representation.
     *
     * @return string
     */
    public function __toString()
    {
        return 'QUEUED';
    }

    /**
     * Returns the value of the specified property.
     *
     * @param string $property Name of the property.
     * @return mixed
     */
    public function __get($property)
    {
        return $property === 'queued';
    }

    /**
     * Checks if the specified property is set.
     *
     * @param string $property Name of the property.
     * @return Boolean
     */
    public function __isset($property)
    {
        return $property === 'queued';
    }
}

/**
 * Represents an error returned by Redis (-ERR replies) during the execution
 * of a command on the server.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseError implements ResponseErrorInterface
{
    private $message;

    /**
     * @param string $message Error message returned by Redis
     */
    public function __construct($message)
    {
        $this->message = $message;
    }

    /**
     * {@inheritdoc}
     */
    public function getMessage()
    {
        return $this->message;
    }

    /**
     * {@inheritdoc}
     */
    public function getErrorType()
    {
        list($errorType, ) = explode(' ', $this->getMessage(), 2);
        return $errorType;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return string
     */
    public function __toString()
    {
        return $this->getMessage();
    }
}

/**
 * Defines a few helper methods.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 * @deprecated Deprecated since v0.8.3.
 */
class Helpers
{
    /**
     * Offers a generic and reusable method to handle exceptions generated by
     * a connection object.
     *
     * @deprecated Deprecated since v0.8.3 - moved in Predis\CommunicationException::handle()
     * @param CommunicationException $exception Exception.
     */
    public static function onCommunicationException(CommunicationException $exception)
    {
        if ($exception->shouldResetConnection()) {
            $connection = $exception->getConnection();

            if ($connection->isConnected()) {
                $connection->disconnect();
            }
        }

        throw $exception;
    }

    /**
     * Normalizes the arguments array passed to a Redis command.
     *
     * @deprecated Deprecated since v0.8.3 - moved in Predis\Command\AbstractCommand::normalizeArguments()
     * @param array $arguments Arguments for a command.
     * @return array
     */
    public static function filterArrayArguments(Array $arguments)
    {
        if (count($arguments) === 1 && is_array($arguments[0])) {
            return $arguments[0];
        }

        return $arguments;
    }

    /**
     * Normalizes the arguments array passed to a variadic Redis command.
     *
     * @deprecated Deprecated since v0.8.3 - moved in Predis\Command\AbstractCommand::normalizeVariadic()
     * @param array $arguments Arguments for a command.
     * @return array
     */
    public static function filterVariadicValues(Array $arguments)
    {
        if (count($arguments) === 2 && is_array($arguments[1])) {
            return array_merge(array($arguments[0]), $arguments[1]);
        }

        return $arguments;
    }
}

/**
 * Main class that exposes the most high-level interface to interact with Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class Client implements ClientInterface
{
    const VERSION = '0.8.4-dev';

    private $options;
    private $profile;
    private $connection;

    /**
     * Initializes a new client with optional connection parameters and client options.
     *
     * @param mixed $parameters Connection parameters for one or multiple servers.
     * @param mixed $options Options that specify certain behaviours for the client.
     */
    public function __construct($parameters = null, $options = null)
    {
        $this->options = $this->filterOptions($options);
        $this->profile = $this->options->profile;
        $this->connection = $this->initializeConnection($parameters);
    }

    /**
     * Creates an instance of Predis\Option\ClientOptions from various types of
     * arguments (string, array, Predis\Profile\ServerProfile) or returns the
     * passed object if it is an instance of Predis\Option\ClientOptions.
     *
     * @param mixed $options Client options.
     * @return ClientOptions
     */
    protected function filterOptions($options)
    {
        if (!isset($options)) {
            return new ClientOptions();
        }

        if (is_array($options)) {
            return new ClientOptions($options);
        }

        if ($options instanceof ClientOptionsInterface) {
            return $options;
        }

        throw new \InvalidArgumentException("Invalid type for client options");
    }

    /**
     * Initializes one or multiple connection (cluster) objects from various
     * types of arguments (string, array) or returns the passed object if it
     * implements Predis\Connection\ConnectionInterface.
     *
     * @param mixed $parameters Connection parameters or instance.
     * @return ConnectionInterface
     */
    protected function initializeConnection($parameters)
    {
        if ($parameters instanceof ConnectionInterface) {
            return $parameters;
        }

        if (is_array($parameters) && isset($parameters[0])) {
            $options = $this->options;
            $replication = isset($options->replication) && $options->replication;
            $connection = $options->{$replication ? 'replication' : 'cluster'};

            return $options->connections->createAggregated($connection, $parameters);
        }

        if (is_callable($parameters)) {
            $connection = call_user_func($parameters, $this->options);

            if (!$connection instanceof ConnectionInterface) {
                throw new \InvalidArgumentException(
                    'Callable parameters must return instances of Predis\Connection\ConnectionInterface'
                );
            }

            return $connection;
        }

        return $this->options->connections->create($parameters);
    }

    /**
     * {@inheritdoc}
     */
    public function getProfile()
    {
        return $this->profile;
    }

    /**
     * {@inheritdoc}
     */
    public function getOptions()
    {
        return $this->options;
    }

    /**
     * Returns the connection factory object used by the client.
     *
     * @return ConnectionFactoryInterface
     */
    public function getConnectionFactory()
    {
        return $this->options->connections;
    }

    /**
     * Returns a new instance of a client for the specified connection when the
     * client is connected to a cluster. The new instance will use the same
     * options of the original client.
     *
     * @return Client
     */
    public function getClientFor($connectionID)
    {
        if (!$connection = $this->getConnectionById($connectionID)) {
            throw new \InvalidArgumentException("Invalid connection ID: '$connectionID'");
        }

        return new static($connection, $this->options);
    }

    /**
     * Opens the connection to the server.
     */
    public function connect()
    {
        $this->connection->connect();
    }

    /**
     * Disconnects from the server.
     */
    public function disconnect()
    {
        $this->connection->disconnect();
    }

    /**
     * Disconnects from the server.
     *
     * This method is an alias of disconnect().
     */
    public function quit()
    {
        $this->disconnect();
    }

    /**
     * Checks if the underlying connection is connected to Redis.
     *
     * @return Boolean True means that the connection is open.
     *                 False means that the connection is closed.
     */
    public function isConnected()
    {
        return $this->connection->isConnected();
    }

    /**
     * {@inheritdoc}
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Retrieves a single connection out of an aggregated connections instance.
     *
     * @param string $connectionId Index or alias of the connection.
     * @return Connection\SingleConnectionInterface
     */
    public function getConnectionById($connectionId)
    {
        if (!$this->connection instanceof AggregatedConnectionInterface) {
            throw new NotSupportedException('Retrieving connections by ID is supported only when using aggregated connections');
        }

        return $this->connection->getConnectionById($connectionId);
    }

    /**
     * Dynamically invokes a Redis command with the specified arguments.
     *
     * @param string $method The name of a Redis command.
     * @param array $arguments The arguments for the command.
     * @return mixed
     */
    public function __call($method, $arguments)
    {
        $command = $this->profile->createCommand($method, $arguments);
        $response = $this->connection->executeCommand($command);

        if ($response instanceof ResponseObjectInterface) {
            if ($response instanceof ResponseErrorInterface) {
                $response = $this->onResponseError($command, $response);
            }

            return $response;
        }

        return $command->parseResponse($response);
    }

    /**
     * {@inheritdoc}
     */
    public function createCommand($method, $arguments = array())
    {
        return $this->profile->createCommand($method, $arguments);
    }

    /**
     * {@inheritdoc}
     */
    public function executeCommand(CommandInterface $command)
    {
        $response = $this->connection->executeCommand($command);

        if ($response instanceof ResponseObjectInterface) {
            if ($response instanceof ResponseErrorInterface) {
                $response = $this->onResponseError($command, $response);
            }

            return $response;
        }

        return $command->parseResponse($response);
    }

    /**
     * Handles -ERR responses returned by Redis.
     *
     * @param CommandInterface $command The command that generated the error.
     * @param ResponseErrorInterface $response The error response instance.
     * @return mixed
     */
    protected function onResponseError(CommandInterface $command, ResponseErrorInterface $response)
    {
        if ($command instanceof ScriptedCommand && $response->getErrorType() === 'NOSCRIPT') {
            $eval = $this->createCommand('eval');
            $eval->setRawArguments($command->getEvalArguments());

            $response = $this->executeCommand($eval);

            if (!$response instanceof ResponseObjectInterface) {
                $response = $command->parseResponse($response);
            }

            return $response;
        }

        if ($this->options->exceptions) {
            throw new ServerException($response->getMessage());
        }

        return $response;
    }

    /**
     * Calls the specified initializer method on $this with 0, 1 or 2 arguments.
     *
     * TODO: Invert $argv and $initializer.
     *
     * @param array $argv Arguments for the initializer.
     * @param string $initializer The initializer method.
     * @return mixed
     */
    private function sharedInitializer($argv, $initializer)
    {
        switch (count($argv)) {
            case 0:
                return $this->$initializer();

            case 1:
                list($arg0) = $argv;
                return is_array($arg0) ? $this->$initializer($arg0) : $this->$initializer(null, $arg0);

            case 2:
                list($arg0, $arg1) = $argv;
                return $this->$initializer($arg0, $arg1);

            default:
                return $this->$initializer($this, $argv);
        }
    }

    /**
     * Creates a new pipeline context and returns it, or returns the results of
     * a pipeline executed inside the optionally provided callable object.
     *
     * @param mixed $arg,... Options for the context, a callable object, or both.
     * @return PipelineContext|array
     */
    public function pipeline(/* arguments */)
    {
        return $this->sharedInitializer(func_get_args(), 'initPipeline');
    }

    /**
     * Pipeline context initializer.
     *
     * @param array $options Options for the context.
     * @param mixed $callable Optional callable object used to execute the context.
     * @return PipelineContext|array
     */
    protected function initPipeline(Array $options = null, $callable = null)
    {
        $executor = isset($options['executor']) ? $options['executor'] : null;

        if (is_callable($executor)) {
            $executor = call_user_func($executor, $this, $options);
        }

        $pipeline = new PipelineContext($this, $executor);
        $replies  = $this->pipelineExecute($pipeline, $callable);

        return $replies;
    }

    /**
     * Executes a pipeline context when a callable object is passed.
     *
     * @param array $options Options of the context initialization.
     * @param mixed $callable Optional callable object used to execute the context.
     * @return PipelineContext|array
     */
    private function pipelineExecute(PipelineContext $pipeline, $callable)
    {
        return isset($callable) ? $pipeline->execute($callable) : $pipeline;
    }

    /**
     * Creates a new transaction context and returns it, or returns the results of
     * a transaction executed inside the optionally provided callable object.
     *
     * @param mixed $arg,... Options for the context, a callable object, or both.
     * @return MultiExecContext|array
     */
    public function multiExec(/* arguments */)
    {
        return $this->sharedInitializer(func_get_args(), 'initMultiExec');
    }

    /**
     * Transaction context initializer.
     *
     * @param array $options Options for the context.
     * @param mixed $callable Optional callable object used to execute the context.
     * @return MultiExecContext|array
     */
    protected function initMultiExec(Array $options = null, $callable = null)
    {
        $transaction = new MultiExecContext($this, $options ?: array());
        return isset($callable) ? $transaction->execute($callable) : $transaction;
    }

    /**
     * Creates a new Publish / Subscribe context and returns it, or executes it
     * inside the optionally provided callable object.
     *
     * @param mixed $arg,... Options for the context, a callable object, or both.
     * @return MultiExecContext|array
     */
    public function pubSub(/* arguments */)
    {
        return $this->sharedInitializer(func_get_args(), 'initPubSub');
    }

    /**
     * Publish / Subscribe context initializer.
     *
     * @param array $options Options for the context.
     * @param mixed $callable Optional callable object used to execute the context.
     * @return PubSubContext
     */
    protected function initPubSub(Array $options = null, $callable = null)
    {
        $pubsub = new PubSubContext($this, $options);

        if (!isset($callable)) {
            return $pubsub;
        }

        foreach ($pubsub as $message) {
            if (call_user_func($callable, $pubsub, $message) === false) {
                $pubsub->closeContext();
            }
        }
    }

    /**
     * Returns a new monitor context.
     *
     * @return MonitorContext
     */
    public function monitor()
    {
        return new MonitorContext($this);
    }
}

/**
 * Exception class that identifies client-side errors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientException extends PredisException
{
}

/**
 * Implements a lightweight PSR-0 compliant autoloader.
 *
 * @author Eric Naeseth <eric@thumbtack.com>
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class Autoloader
{
    private $directory;
    private $prefix;
    private $prefixLength;

    /**
     * @param string $baseDirectory Base directory where the source files are located.
     */
    public function __construct($baseDirectory = __DIR__)
    {
        $this->directory = $baseDirectory;
        $this->prefix = __NAMESPACE__ . '\\';
        $this->prefixLength = strlen($this->prefix);
    }

    /**
     * Registers the autoloader class with the PHP SPL autoloader.
     *
     * @param boolean $prepend Prepend the autoloader on the stack instead of appending it.
     */
    public static function register($prepend = false)
    {
        spl_autoload_register(array(new self, 'autoload'), true, $prepend);
    }

    /**
     * Loads a class from a file using its fully qualified name.
     *
     * @param string $className Fully qualified name of a class.
     */
    public function autoload($className)
    {
        if (0 === strpos($className, $this->prefix)) {
            $parts = explode('\\', substr($className, $this->prefixLength));
            $filepath = $this->directory.DIRECTORY_SEPARATOR.implode(DIRECTORY_SEPARATOR, $parts).'.php';

            if (is_file($filepath)) {
                require($filepath);
            }
        }
    }
}

/**
 * Exception class generated when trying to use features not
 * supported by certain classes or abstractions.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class NotSupportedException extends PredisException
{
}

/* --------------------------------------------------------------------------- */

namespace Predis\Option;

use Predis\Connection\ClusterConnectionInterface;
use Predis\Connection\PredisCluster;
use Predis\Connection\RedisCluster;
use Predis\Connection\ConnectionFactory;
use Predis\Connection\ConnectionFactoryInterface;
use Predis\Command\Processor\KeyPrefixProcessor;
use Predis\Profile\ServerProfile;
use Predis\Profile\ServerProfileInterface;
use Predis\Connection\MasterSlaveReplication;
use Predis\Connection\ReplicationConnectionInterface;

/**
 * Interface that defines a client option.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface OptionInterface
{
    /**
     * Filters (and optionally converts) the passed value.
     *
     * @param mixed $value Input value.
     * @return mixed
     */
    public function filter(ClientOptionsInterface $options, $value);

    /**
     * Returns a default value for the option.
     *
     * @param mixed $value Input value.
     * @return mixed
     */
    public function getDefault(ClientOptionsInterface $options);

    /**
     * Filters a value and, if no value is specified, returns
     * the default one defined by the option.
     *
     * @param mixed $value Input value.
     * @return mixed
     */
    public function __invoke(ClientOptionsInterface $options, $value);
}

/**
 * Implements a client option.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class AbstractOption implements OptionInterface
{
    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        return $value;
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function __invoke(ClientOptionsInterface $options, $value)
    {
        if (isset($value)) {
            return $this->filter($options, $value);
        }

        return $this->getDefault($options);
    }
}

/**
 * Marker interface defining a client options bag.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ClientOptionsInterface
{
}

/**
 * Option class that returns a replication connection be used by a client.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientReplication extends AbstractOption
{
    /**
     * Checks if the specified value is a valid instance of ReplicationConnectionInterface.
     *
     * @param ReplicationConnectionInterface $connection Instance of a replication connection.
     * @return ReplicationConnectionInterface
     */
    protected function checkInstance($connection)
    {
        if (!$connection instanceof ReplicationConnectionInterface) {
            throw new \InvalidArgumentException('Instance of Predis\Connection\ReplicationConnectionInterface expected');
        }

        return $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        if (is_callable($value)) {
            $connection = call_user_func($value, $options, $this);

            if (!$connection instanceof ReplicationConnectionInterface) {
                throw new \InvalidArgumentException('Instance of Predis\Connection\ReplicationConnectionInterface expected');
            }

            return $connection;
        }

        if (is_string($value)) {
            if (!class_exists($value)) {
                throw new \InvalidArgumentException("Class $value does not exist");
            }

            if (!($connection = new $value()) instanceof ReplicationConnectionInterface) {
                throw new \InvalidArgumentException('Instance of Predis\Connection\ReplicationConnectionInterface expected');
            }

            return $connection;
        }

        if ($value == true) {
            return $this->getDefault($options);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        return new MasterSlaveReplication();
    }
}

/**
 * Implements a generic class used to dynamically define a client option.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class CustomOption implements OptionInterface
{
    private $filter;
    private $default;

    /**
     * @param array $options List of options
     */
    public function __construct(Array $options = array())
    {
        $this->filter = $this->ensureCallable($options, 'filter');
        $this->default = $this->ensureCallable($options, 'default');
    }

    /**
     * Checks if the specified value in the options array is a callable object.
     *
     * @param array $options Array of options
     * @param string $key Target option.
     */
    private function ensureCallable($options, $key)
    {
        if (!isset($options[$key])) {
            return;
        }

        if (is_callable($callable = $options[$key])) {
            return $callable;
        }

        throw new \InvalidArgumentException("The parameter $key must be callable");
    }

    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        if (isset($value)) {
            if ($this->filter === null) {
                return $value;
            }

            return call_user_func($this->filter, $options, $value);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        if (!isset($this->default)) {
            return;
        }

        return call_user_func($this->default, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function __invoke(ClientOptionsInterface $options, $value)
    {
        if (isset($value)) {
            return $this->filter($options, $value);
        }

        return $this->getDefault($options);
    }
}

/**
 * Option class that returns a connection cluster to be used by a client.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientCluster extends AbstractOption
{
    /**
     * Checks if the specified value is a valid instance of ClusterConnectionInterface.
     *
     * @param ClusterConnectionInterface $cluster Instance of a connection cluster.
     * @return ClusterConnectionInterface
     */
    protected function checkInstance($cluster)
    {
        if (!$cluster instanceof ClusterConnectionInterface) {
            throw new \InvalidArgumentException('Instance of Predis\Connection\ClusterConnectionInterface expected');
        }

        return $cluster;
    }

    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        if (is_callable($value)) {
            return $this->checkInstance(call_user_func($value, $options, $this));
        }

        $initializer = $this->getInitializer($options, $value);

        return $this->checkInstance($initializer());
    }

    /**
     * Returns an initializer for the specified FQN or type.
     *
     * @param string $fqnOrType Type of cluster or FQN of a class implementing ClusterConnectionInterface.
     * @param ClientOptionsInterface $options Instance of the client options.
     * @return \Closure
     */
    protected function getInitializer(ClientOptionsInterface $options, $fqnOrType)
    {
        switch ($fqnOrType) {
            case 'predis':
                return function () {
                    return new PredisCluster();
                };

            case 'redis':
                return function () use ($options) {
                    $connectionFactory = $options->connections;
                    $cluster = new RedisCluster($connectionFactory);

                    return $cluster;
                };

            default:
                // TODO: we should not even allow non-string values here.
                if (is_string($fqnOrType) && !class_exists($fqnOrType)) {
                    throw new \InvalidArgumentException("Class $fqnOrType does not exist");
                }
                return function () use ($fqnOrType) {
                    return new $fqnOrType();
                };
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        return new PredisCluster();
    }
}

/**
 * Option class that handles server profiles to be used by a client.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientProfile extends AbstractOption
{
    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        if (is_string($value)) {
            $value = ServerProfile::get($value);

            if (isset($options->prefix)) {
                $value->setProcessor($options->prefix);
            }
        }

        if (is_callable($value)) {
            $value = call_user_func($value, $options, $this);
        }

        if (!$value instanceof ServerProfileInterface) {
            throw new \InvalidArgumentException('Invalid value for the profile option');
        }

        return $value;
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        $profile = ServerProfile::getDefault();

        if (isset($options->prefix)) {
            $profile->setProcessor($options->prefix);
        }

        return $profile;
    }
}

/**
 * Option class that handles the prefixing of keys in commands.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientPrefix extends AbstractOption
{
    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        return new KeyPrefixProcessor($value);
    }
}

/**
 * Class that manages client options with filtering and conversion.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientOptions implements ClientOptionsInterface
{
    private $handlers;
    private $defined;
    private $options = array();

    /**
     * @param array $options Array of client options.
     */
    public function __construct(Array $options = array())
    {
        $this->handlers = $this->initialize($options);
        $this->defined = array_fill_keys(array_keys($options), true);
    }

    /**
     * Ensures that the default options are initialized.
     *
     * @return array
     */
    protected function getDefaultOptions()
    {
        return array(
            'profile' => new ClientProfile(),
            'connections' => new ClientConnectionFactory(),
            'cluster' => new ClientCluster(),
            'replication' => new ClientReplication(),
            'prefix' => new ClientPrefix(),
            'exceptions' => new ClientExceptions(),
        );
    }

    /**
     * Initializes client options handlers.
     *
     * @param array $options List of client options values.
     * @return array
     */
    protected function initialize(Array $options)
    {
        $handlers = $this->getDefaultOptions();

        foreach ($options as $option => $value) {
            if (isset($handlers[$option])) {
                $handler = $handlers[$option];
                $handlers[$option] = function ($options) use ($handler, $value) {
                    return $handler->filter($options, $value);
                };
            } else {
                $this->options[$option] = $value;
            }
        }

        return $handlers;
    }

    /**
     * Checks if the specified option is set.
     *
     * @param string $option Name of the option.
     * @return Boolean
     */
    public function __isset($option)
    {
        return isset($this->defined[$option]);
    }

    /**
     * Returns the value of the specified option.
     *
     * @param string $option Name of the option.
     * @return mixed
     */
    public function __get($option)
    {
        if (isset($this->options[$option])) {
            return $this->options[$option];
        }

        if (isset($this->handlers[$option])) {
            $handler = $this->handlers[$option];
            $value = $handler instanceof OptionInterface ? $handler->getDefault($this) : $handler($this);
            $this->options[$option] = $value;

            return $value;
        }
    }

    /**
     * Returns the default value for the specified option.
     *
     * @param string|OptionInterface $option Name or instance of the option.
     * @return mixed
     */
    public function getDefault($option)
    {
        if ($option instanceof OptionInterface) {
            return $option->getDefault($this);
        }

        $options = $this->getDefaultOptions();

        if (isset($options[$option])) {
            return $options[$option]->getDefault($this);
        }
    }
}

/**
 * Option class that returns a connection factory to be used by a client.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientConnectionFactory extends AbstractOption
{
    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        if ($value instanceof ConnectionFactoryInterface) {
            return $value;
        }

        if (is_array($value)) {
            $factory = $this->getDefault($options);

            foreach ($value as $scheme => $initializer) {
                $factory->define($scheme, $initializer);
            }

            return $factory;
        }

        if (is_callable($value)) {
            $factory = call_user_func($value, $options, $this);

            if (!$factory instanceof ConnectionFactoryInterface) {
                throw new \InvalidArgumentException('Instance of Predis\Connection\ConnectionFactoryInterface expected');
            }

            return $factory;
        }

        if (@class_exists($value)) {
            $factory = new $value();

            if (!$factory instanceof ConnectionFactoryInterface) {
                throw new \InvalidArgumentException("Class $value must be an instance of Predis\Connection\ConnectionFactoryInterface");
            }

            return $factory;
        }

        throw new \InvalidArgumentException('Invalid value for the connections option');
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        return new ConnectionFactory($options->profile);
    }
}

/**
 * Option class used to specify if the client should throw server exceptions.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ClientExceptions extends AbstractOption
{
    /**
     * {@inheritdoc}
     */
    public function filter(ClientOptionsInterface $options, $value)
    {
        return (bool) $value;
    }

    /**
     * {@inheritdoc}
     */
    public function getDefault(ClientOptionsInterface $options)
    {
        return true;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Profile;

use Predis\ClientException;
use Predis\Command\Processor\CommandProcessingInterface;
use Predis\Command\Processor\CommandProcessorInterface;
use Predis\Command\CommandInterface;

/**
 * A server profile defines features and commands supported by certain
 * versions of Redis. Instances of Predis\Client should use a server
 * profile matching the version of Redis in use.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ServerProfileInterface
{
    /**
     * Gets a profile version corresponding to a Redis version.
     *
     * @return string
     */
    public function getVersion();

    /**
     * Checks if the profile supports the specified command.
     *
     * @param string $command Command ID.
     * @return Boolean
     */
    public function supportsCommand($command);

    /**
     * Checks if the profile supports the specified list of commands.
     *
     * @param array $commands List of command IDs.
     * @return string
     */
    public function supportsCommands(Array $commands);

    /**
     * Creates a new command instance.
     *
     * @param string $method Command ID.
     * @param array $arguments Arguments for the command.
     * @return CommandInterface
     */
    public function createCommand($method, $arguments = array());
}

/**
 * Base class that implements common functionalities of server profiles.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class ServerProfile implements ServerProfileInterface, CommandProcessingInterface
{
    private static $profiles;

    private $commands;
    private $processor;

    /**
     *
     */
    public function __construct()
    {
        $this->commands = $this->getSupportedCommands();
    }

    /**
     * Returns a map of all the commands supported by the profile and their
     * actual PHP classes.
     *
     * @return array
     */
    protected abstract function getSupportedCommands();

    /**
     * Returns the default server profile.
     *
     * @return ServerProfileInterface
     */
    public static function getDefault()
    {
        return self::get('default');
    }

    /**
     * Returns the development server profile.
     *
     * @return ServerProfileInterface
     */
    public static function getDevelopment()
    {
        return self::get('dev');
    }

    /**
     * Returns a map of all the server profiles supported by default and their
     * actual PHP classes.
     *
     * @return array
     */
    private static function getDefaultProfiles()
    {
        return array(
            '1.2'     => 'Predis\Profile\ServerVersion12',
            '2.0'     => 'Predis\Profile\ServerVersion20',
            '2.2'     => 'Predis\Profile\ServerVersion22',
            '2.4'     => 'Predis\Profile\ServerVersion24',
            '2.6'     => 'Predis\Profile\ServerVersion26',
            'default' => 'Predis\Profile\ServerVersion26',
            'dev'     => 'Predis\Profile\ServerVersionNext',
        );
    }

    /**
     * Registers a new server profile.
     *
     * @param string $alias Profile version or alias.
     * @param string $profileClass FQN of a class implementing Predis\Profile\ServerProfileInterface.
     */
    public static function define($alias, $profileClass)
    {
        if (!isset(self::$profiles)) {
            self::$profiles = self::getDefaultProfiles();
        }

        $profileReflection = new \ReflectionClass($profileClass);

        if (!$profileReflection->isSubclassOf('Predis\Profile\ServerProfileInterface')) {
            throw new \InvalidArgumentException("Cannot register '$profileClass' as it is not a valid profile class");
        }

        self::$profiles[$alias] = $profileClass;
    }

    /**
     * Returns the specified server profile.
     *
     * @param string $version Profile version or alias.
     * @return ServerProfileInterface
     */
    public static function get($version)
    {
        if (!isset(self::$profiles)) {
            self::$profiles = self::getDefaultProfiles();
        }

        if (!isset(self::$profiles[$version])) {
            throw new ClientException("Unknown server profile: $version");
        }

        $profile = self::$profiles[$version];

        return new $profile();
    }

    /**
     * {@inheritdoc}
     */
    public function supportsCommands(Array $commands)
    {
        foreach ($commands as $command) {
            if (!$this->supportsCommand($command)) {
                return false;
            }
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function supportsCommand($command)
    {
        return isset($this->commands[strtolower($command)]);
    }

    /**
     * Returns the FQN of the class that represent the specified command ID
     * registered in the current server profile.
     *
     * @param string $command Command ID.
     * @return string
     */
    public function getCommandClass($command)
    {
        if (isset($this->commands[$command = strtolower($command)])) {
            return $this->commands[$command];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createCommand($method, $arguments = array())
    {
        $method = strtolower($method);

        if (!isset($this->commands[$method])) {
            throw new ClientException("'$method' is not a registered Redis command");
        }

        $commandClass = $this->commands[$method];
        $command = new $commandClass();
        $command->setArguments($arguments);

        if (isset($this->processor)) {
            $this->processor->process($command);
        }

        return $command;
    }

    /**
     * Defines a new commands in the server profile.
     *
     * @param string $alias Command ID.
     * @param string $command FQN of a class implementing Predis\Command\CommandInterface.
     */
    public function defineCommand($alias, $command)
    {
        $commandReflection = new \ReflectionClass($command);

        if (!$commandReflection->isSubclassOf('Predis\Command\CommandInterface')) {
            throw new \InvalidArgumentException("Cannot register '$command' as it is not a valid Redis command");
        }

        $this->commands[strtolower($alias)] = $command;
    }

    /**
     * {@inheritdoc}
     */
    public function setProcessor(CommandProcessorInterface $processor = null)
    {
        $this->processor = $processor;
    }

    /**
     * {@inheritdoc}
     */
    public function getProcessor()
    {
        return $this->processor;
    }

    /**
     * Returns the version of server profile as its string representation.
     *
     * @return string
     */
    public function __toString()
    {
        return $this->getVersion();
    }
}

/**
 * Server profile for Redis v2.6.x.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersion26 extends ServerProfile
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '2.6';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array(
            /* ---------------- Redis 1.2 ---------------- */

            /* commands operating on the key space */
            'exists'                    => 'Predis\Command\KeyExists',
            'del'                       => 'Predis\Command\KeyDelete',
            'type'                      => 'Predis\Command\KeyType',
            'keys'                      => 'Predis\Command\KeyKeys',
            'randomkey'                 => 'Predis\Command\KeyRandom',
            'rename'                    => 'Predis\Command\KeyRename',
            'renamenx'                  => 'Predis\Command\KeyRenamePreserve',
            'expire'                    => 'Predis\Command\KeyExpire',
            'expireat'                  => 'Predis\Command\KeyExpireAt',
            'ttl'                       => 'Predis\Command\KeyTimeToLive',
            'move'                      => 'Predis\Command\KeyMove',
            'sort'                      => 'Predis\Command\KeySort',
            'dump'                      => 'Predis\Command\KeyDump',
            'restore'                   => 'Predis\Command\KeyRestore',

            /* commands operating on string values */
            'set'                       => 'Predis\Command\StringSet',
            'setnx'                     => 'Predis\Command\StringSetPreserve',
            'mset'                      => 'Predis\Command\StringSetMultiple',
            'msetnx'                    => 'Predis\Command\StringSetMultiplePreserve',
            'get'                       => 'Predis\Command\StringGet',
            'mget'                      => 'Predis\Command\StringGetMultiple',
            'getset'                    => 'Predis\Command\StringGetSet',
            'incr'                      => 'Predis\Command\StringIncrement',
            'incrby'                    => 'Predis\Command\StringIncrementBy',
            'decr'                      => 'Predis\Command\StringDecrement',
            'decrby'                    => 'Predis\Command\StringDecrementBy',

            /* commands operating on lists */
            'rpush'                     => 'Predis\Command\ListPushTail',
            'lpush'                     => 'Predis\Command\ListPushHead',
            'llen'                      => 'Predis\Command\ListLength',
            'lrange'                    => 'Predis\Command\ListRange',
            'ltrim'                     => 'Predis\Command\ListTrim',
            'lindex'                    => 'Predis\Command\ListIndex',
            'lset'                      => 'Predis\Command\ListSet',
            'lrem'                      => 'Predis\Command\ListRemove',
            'lpop'                      => 'Predis\Command\ListPopFirst',
            'rpop'                      => 'Predis\Command\ListPopLast',
            'rpoplpush'                 => 'Predis\Command\ListPopLastPushHead',

            /* commands operating on sets */
            'sadd'                      => 'Predis\Command\SetAdd',
            'srem'                      => 'Predis\Command\SetRemove',
            'spop'                      => 'Predis\Command\SetPop',
            'smove'                     => 'Predis\Command\SetMove',
            'scard'                     => 'Predis\Command\SetCardinality',
            'sismember'                 => 'Predis\Command\SetIsMember',
            'sinter'                    => 'Predis\Command\SetIntersection',
            'sinterstore'               => 'Predis\Command\SetIntersectionStore',
            'sunion'                    => 'Predis\Command\SetUnion',
            'sunionstore'               => 'Predis\Command\SetUnionStore',
            'sdiff'                     => 'Predis\Command\SetDifference',
            'sdiffstore'                => 'Predis\Command\SetDifferenceStore',
            'smembers'                  => 'Predis\Command\SetMembers',
            'srandmember'               => 'Predis\Command\SetRandomMember',

            /* commands operating on sorted sets */
            'zadd'                      => 'Predis\Command\ZSetAdd',
            'zincrby'                   => 'Predis\Command\ZSetIncrementBy',
            'zrem'                      => 'Predis\Command\ZSetRemove',
            'zrange'                    => 'Predis\Command\ZSetRange',
            'zrevrange'                 => 'Predis\Command\ZSetReverseRange',
            'zrangebyscore'             => 'Predis\Command\ZSetRangeByScore',
            'zcard'                     => 'Predis\Command\ZSetCardinality',
            'zscore'                    => 'Predis\Command\ZSetScore',
            'zremrangebyscore'          => 'Predis\Command\ZSetRemoveRangeByScore',

            /* connection related commands */
            'ping'                      => 'Predis\Command\ConnectionPing',
            'auth'                      => 'Predis\Command\ConnectionAuth',
            'select'                    => 'Predis\Command\ConnectionSelect',
            'echo'                      => 'Predis\Command\ConnectionEcho',
            'quit'                      => 'Predis\Command\ConnectionQuit',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfo',
            'slaveof'                   => 'Predis\Command\ServerSlaveOf',
            'monitor'                   => 'Predis\Command\ServerMonitor',
            'dbsize'                    => 'Predis\Command\ServerDatabaseSize',
            'flushdb'                   => 'Predis\Command\ServerFlushDatabase',
            'flushall'                  => 'Predis\Command\ServerFlushAll',
            'save'                      => 'Predis\Command\ServerSave',
            'bgsave'                    => 'Predis\Command\ServerBackgroundSave',
            'lastsave'                  => 'Predis\Command\ServerLastSave',
            'shutdown'                  => 'Predis\Command\ServerShutdown',
            'bgrewriteaof'              => 'Predis\Command\ServerBackgroundRewriteAOF',


            /* ---------------- Redis 2.0 ---------------- */

            /* commands operating on string values */
            'setex'                     => 'Predis\Command\StringSetExpire',
            'append'                    => 'Predis\Command\StringAppend',
            'substr'                    => 'Predis\Command\StringSubstr',

            /* commands operating on lists */
            'blpop'                     => 'Predis\Command\ListPopFirstBlocking',
            'brpop'                     => 'Predis\Command\ListPopLastBlocking',

            /* commands operating on sorted sets */
            'zunionstore'               => 'Predis\Command\ZSetUnionStore',
            'zinterstore'               => 'Predis\Command\ZSetIntersectionStore',
            'zcount'                    => 'Predis\Command\ZSetCount',
            'zrank'                     => 'Predis\Command\ZSetRank',
            'zrevrank'                  => 'Predis\Command\ZSetReverseRank',
            'zremrangebyrank'           => 'Predis\Command\ZSetRemoveRangeByRank',

            /* commands operating on hashes */
            'hset'                      => 'Predis\Command\HashSet',
            'hsetnx'                    => 'Predis\Command\HashSetPreserve',
            'hmset'                     => 'Predis\Command\HashSetMultiple',
            'hincrby'                   => 'Predis\Command\HashIncrementBy',
            'hget'                      => 'Predis\Command\HashGet',
            'hmget'                     => 'Predis\Command\HashGetMultiple',
            'hdel'                      => 'Predis\Command\HashDelete',
            'hexists'                   => 'Predis\Command\HashExists',
            'hlen'                      => 'Predis\Command\HashLength',
            'hkeys'                     => 'Predis\Command\HashKeys',
            'hvals'                     => 'Predis\Command\HashValues',
            'hgetall'                   => 'Predis\Command\HashGetAll',

            /* transactions */
            'multi'                     => 'Predis\Command\TransactionMulti',
            'exec'                      => 'Predis\Command\TransactionExec',
            'discard'                   => 'Predis\Command\TransactionDiscard',

            /* publish - subscribe */
            'subscribe'                 => 'Predis\Command\PubSubSubscribe',
            'unsubscribe'               => 'Predis\Command\PubSubUnsubscribe',
            'psubscribe'                => 'Predis\Command\PubSubSubscribeByPattern',
            'punsubscribe'              => 'Predis\Command\PubSubUnsubscribeByPattern',
            'publish'                   => 'Predis\Command\PubSubPublish',

            /* remote server control commands */
            'config'                    => 'Predis\Command\ServerConfig',


            /* ---------------- Redis 2.2 ---------------- */

            /* commands operating on the key space */
            'persist'                   => 'Predis\Command\KeyPersist',

            /* commands operating on string values */
            'strlen'                    => 'Predis\Command\StringStrlen',
            'setrange'                  => 'Predis\Command\StringSetRange',
            'getrange'                  => 'Predis\Command\StringGetRange',
            'setbit'                    => 'Predis\Command\StringSetBit',
            'getbit'                    => 'Predis\Command\StringGetBit',

            /* commands operating on lists */
            'rpushx'                    => 'Predis\Command\ListPushTailX',
            'lpushx'                    => 'Predis\Command\ListPushHeadX',
            'linsert'                   => 'Predis\Command\ListInsert',
            'brpoplpush'                => 'Predis\Command\ListPopLastPushHeadBlocking',

            /* commands operating on sorted sets */
            'zrevrangebyscore'          => 'Predis\Command\ZSetReverseRangeByScore',

            /* transactions */
            'watch'                     => 'Predis\Command\TransactionWatch',
            'unwatch'                   => 'Predis\Command\TransactionUnwatch',

            /* remote server control commands */
            'object'                    => 'Predis\Command\ServerObject',
            'slowlog'                   => 'Predis\Command\ServerSlowlog',


            /* ---------------- Redis 2.4 ---------------- */

            /* remote server control commands */
            'client'                    => 'Predis\Command\ServerClient',


            /* ---------------- Redis 2.6 ---------------- */

            /* commands operating on the key space */
            'pttl'                      => 'Predis\Command\KeyPreciseTimeToLive',
            'pexpire'                   => 'Predis\Command\KeyPreciseExpire',
            'pexpireat'                 => 'Predis\Command\KeyPreciseExpireAt',

            /* commands operating on string values */
            'psetex'                    => 'Predis\Command\StringPreciseSetExpire',
            'incrbyfloat'               => 'Predis\Command\StringIncrementByFloat',
            'bitop'                     => 'Predis\Command\StringBitOp',
            'bitcount'                  => 'Predis\Command\StringBitCount',

            /* commands operating on hashes */
            'hincrbyfloat'              => 'Predis\Command\HashIncrementByFloat',

            /* scripting */
            'eval'                      => 'Predis\Command\ServerEval',
            'evalsha'                   => 'Predis\Command\ServerEvalSHA',
            'script'                    => 'Predis\Command\ServerScript',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfoV26x',
            'time'                      => 'Predis\Command\ServerTime',
        );
    }
}

/**
 * Server profile for the current unstable version of Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersionNext extends ServerVersion26
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '2.8';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array_merge(parent::getSupportedCommands(), array());
    }
}

/**
 * Server profile for Redis v2.4.x.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersion24 extends ServerProfile
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '2.4';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array(
            /* ---------------- Redis 1.2 ---------------- */

            /* commands operating on the key space */
            'exists'                    => 'Predis\Command\KeyExists',
            'del'                       => 'Predis\Command\KeyDelete',
            'type'                      => 'Predis\Command\KeyType',
            'keys'                      => 'Predis\Command\KeyKeys',
            'randomkey'                 => 'Predis\Command\KeyRandom',
            'rename'                    => 'Predis\Command\KeyRename',
            'renamenx'                  => 'Predis\Command\KeyRenamePreserve',
            'expire'                    => 'Predis\Command\KeyExpire',
            'expireat'                  => 'Predis\Command\KeyExpireAt',
            'ttl'                       => 'Predis\Command\KeyTimeToLive',
            'move'                      => 'Predis\Command\KeyMove',
            'sort'                      => 'Predis\Command\KeySort',

            /* commands operating on string values */
            'set'                       => 'Predis\Command\StringSet',
            'setnx'                     => 'Predis\Command\StringSetPreserve',
            'mset'                      => 'Predis\Command\StringSetMultiple',
            'msetnx'                    => 'Predis\Command\StringSetMultiplePreserve',
            'get'                       => 'Predis\Command\StringGet',
            'mget'                      => 'Predis\Command\StringGetMultiple',
            'getset'                    => 'Predis\Command\StringGetSet',
            'incr'                      => 'Predis\Command\StringIncrement',
            'incrby'                    => 'Predis\Command\StringIncrementBy',
            'decr'                      => 'Predis\Command\StringDecrement',
            'decrby'                    => 'Predis\Command\StringDecrementBy',

            /* commands operating on lists */
            'rpush'                     => 'Predis\Command\ListPushTail',
            'lpush'                     => 'Predis\Command\ListPushHead',
            'llen'                      => 'Predis\Command\ListLength',
            'lrange'                    => 'Predis\Command\ListRange',
            'ltrim'                     => 'Predis\Command\ListTrim',
            'lindex'                    => 'Predis\Command\ListIndex',
            'lset'                      => 'Predis\Command\ListSet',
            'lrem'                      => 'Predis\Command\ListRemove',
            'lpop'                      => 'Predis\Command\ListPopFirst',
            'rpop'                      => 'Predis\Command\ListPopLast',
            'rpoplpush'                 => 'Predis\Command\ListPopLastPushHead',

            /* commands operating on sets */
            'sadd'                      => 'Predis\Command\SetAdd',
            'srem'                      => 'Predis\Command\SetRemove',
            'spop'                      => 'Predis\Command\SetPop',
            'smove'                     => 'Predis\Command\SetMove',
            'scard'                     => 'Predis\Command\SetCardinality',
            'sismember'                 => 'Predis\Command\SetIsMember',
            'sinter'                    => 'Predis\Command\SetIntersection',
            'sinterstore'               => 'Predis\Command\SetIntersectionStore',
            'sunion'                    => 'Predis\Command\SetUnion',
            'sunionstore'               => 'Predis\Command\SetUnionStore',
            'sdiff'                     => 'Predis\Command\SetDifference',
            'sdiffstore'                => 'Predis\Command\SetDifferenceStore',
            'smembers'                  => 'Predis\Command\SetMembers',
            'srandmember'               => 'Predis\Command\SetRandomMember',

            /* commands operating on sorted sets */
            'zadd'                      => 'Predis\Command\ZSetAdd',
            'zincrby'                   => 'Predis\Command\ZSetIncrementBy',
            'zrem'                      => 'Predis\Command\ZSetRemove',
            'zrange'                    => 'Predis\Command\ZSetRange',
            'zrevrange'                 => 'Predis\Command\ZSetReverseRange',
            'zrangebyscore'             => 'Predis\Command\ZSetRangeByScore',
            'zcard'                     => 'Predis\Command\ZSetCardinality',
            'zscore'                    => 'Predis\Command\ZSetScore',
            'zremrangebyscore'          => 'Predis\Command\ZSetRemoveRangeByScore',

            /* connection related commands */
            'ping'                      => 'Predis\Command\ConnectionPing',
            'auth'                      => 'Predis\Command\ConnectionAuth',
            'select'                    => 'Predis\Command\ConnectionSelect',
            'echo'                      => 'Predis\Command\ConnectionEcho',
            'quit'                      => 'Predis\Command\ConnectionQuit',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfo',
            'slaveof'                   => 'Predis\Command\ServerSlaveOf',
            'monitor'                   => 'Predis\Command\ServerMonitor',
            'dbsize'                    => 'Predis\Command\ServerDatabaseSize',
            'flushdb'                   => 'Predis\Command\ServerFlushDatabase',
            'flushall'                  => 'Predis\Command\ServerFlushAll',
            'save'                      => 'Predis\Command\ServerSave',
            'bgsave'                    => 'Predis\Command\ServerBackgroundSave',
            'lastsave'                  => 'Predis\Command\ServerLastSave',
            'shutdown'                  => 'Predis\Command\ServerShutdown',
            'bgrewriteaof'              => 'Predis\Command\ServerBackgroundRewriteAOF',


            /* ---------------- Redis 2.0 ---------------- */

            /* commands operating on string values */
            'setex'                     => 'Predis\Command\StringSetExpire',
            'append'                    => 'Predis\Command\StringAppend',
            'substr'                    => 'Predis\Command\StringSubstr',

            /* commands operating on lists */
            'blpop'                     => 'Predis\Command\ListPopFirstBlocking',
            'brpop'                     => 'Predis\Command\ListPopLastBlocking',

            /* commands operating on sorted sets */
            'zunionstore'               => 'Predis\Command\ZSetUnionStore',
            'zinterstore'               => 'Predis\Command\ZSetIntersectionStore',
            'zcount'                    => 'Predis\Command\ZSetCount',
            'zrank'                     => 'Predis\Command\ZSetRank',
            'zrevrank'                  => 'Predis\Command\ZSetReverseRank',
            'zremrangebyrank'           => 'Predis\Command\ZSetRemoveRangeByRank',

            /* commands operating on hashes */
            'hset'                      => 'Predis\Command\HashSet',
            'hsetnx'                    => 'Predis\Command\HashSetPreserve',
            'hmset'                     => 'Predis\Command\HashSetMultiple',
            'hincrby'                   => 'Predis\Command\HashIncrementBy',
            'hget'                      => 'Predis\Command\HashGet',
            'hmget'                     => 'Predis\Command\HashGetMultiple',
            'hdel'                      => 'Predis\Command\HashDelete',
            'hexists'                   => 'Predis\Command\HashExists',
            'hlen'                      => 'Predis\Command\HashLength',
            'hkeys'                     => 'Predis\Command\HashKeys',
            'hvals'                     => 'Predis\Command\HashValues',
            'hgetall'                   => 'Predis\Command\HashGetAll',

            /* transactions */
            'multi'                     => 'Predis\Command\TransactionMulti',
            'exec'                      => 'Predis\Command\TransactionExec',
            'discard'                   => 'Predis\Command\TransactionDiscard',

            /* publish - subscribe */
            'subscribe'                 => 'Predis\Command\PubSubSubscribe',
            'unsubscribe'               => 'Predis\Command\PubSubUnsubscribe',
            'psubscribe'                => 'Predis\Command\PubSubSubscribeByPattern',
            'punsubscribe'              => 'Predis\Command\PubSubUnsubscribeByPattern',
            'publish'                   => 'Predis\Command\PubSubPublish',

            /* remote server control commands */
            'config'                    => 'Predis\Command\ServerConfig',


            /* ---------------- Redis 2.2 ---------------- */

            /* commands operating on the key space */
            'persist'                   => 'Predis\Command\KeyPersist',

            /* commands operating on string values */
            'strlen'                    => 'Predis\Command\StringStrlen',
            'setrange'                  => 'Predis\Command\StringSetRange',
            'getrange'                  => 'Predis\Command\StringGetRange',
            'setbit'                    => 'Predis\Command\StringSetBit',
            'getbit'                    => 'Predis\Command\StringGetBit',

            /* commands operating on lists */
            'rpushx'                    => 'Predis\Command\ListPushTailX',
            'lpushx'                    => 'Predis\Command\ListPushHeadX',
            'linsert'                   => 'Predis\Command\ListInsert',
            'brpoplpush'                => 'Predis\Command\ListPopLastPushHeadBlocking',

            /* commands operating on sorted sets */
            'zrevrangebyscore'          => 'Predis\Command\ZSetReverseRangeByScore',

            /* transactions */
            'watch'                     => 'Predis\Command\TransactionWatch',
            'unwatch'                   => 'Predis\Command\TransactionUnwatch',

            /* remote server control commands */
            'object'                    => 'Predis\Command\ServerObject',
            'slowlog'                   => 'Predis\Command\ServerSlowlog',


            /* ---------------- Redis 2.4 ---------------- */

            /* remote server control commands */
            'client'                    => 'Predis\Command\ServerClient',
        );
    }
}

/**
 * Server profile for Redis v2.0.x.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersion20 extends ServerProfile
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '2.0';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array(
            /* ---------------- Redis 1.2 ---------------- */

            /* commands operating on the key space */
            'exists'                    => 'Predis\Command\KeyExists',
            'del'                       => 'Predis\Command\KeyDelete',
            'type'                      => 'Predis\Command\KeyType',
            'keys'                      => 'Predis\Command\KeyKeys',
            'randomkey'                 => 'Predis\Command\KeyRandom',
            'rename'                    => 'Predis\Command\KeyRename',
            'renamenx'                  => 'Predis\Command\KeyRenamePreserve',
            'expire'                    => 'Predis\Command\KeyExpire',
            'expireat'                  => 'Predis\Command\KeyExpireAt',
            'ttl'                       => 'Predis\Command\KeyTimeToLive',
            'move'                      => 'Predis\Command\KeyMove',
            'sort'                      => 'Predis\Command\KeySort',

            /* commands operating on string values */
            'set'                       => 'Predis\Command\StringSet',
            'setnx'                     => 'Predis\Command\StringSetPreserve',
            'mset'                      => 'Predis\Command\StringSetMultiple',
            'msetnx'                    => 'Predis\Command\StringSetMultiplePreserve',
            'get'                       => 'Predis\Command\StringGet',
            'mget'                      => 'Predis\Command\StringGetMultiple',
            'getset'                    => 'Predis\Command\StringGetSet',
            'incr'                      => 'Predis\Command\StringIncrement',
            'incrby'                    => 'Predis\Command\StringIncrementBy',
            'decr'                      => 'Predis\Command\StringDecrement',
            'decrby'                    => 'Predis\Command\StringDecrementBy',

            /* commands operating on lists */
            'rpush'                     => 'Predis\Command\ListPushTail',
            'lpush'                     => 'Predis\Command\ListPushHead',
            'llen'                      => 'Predis\Command\ListLength',
            'lrange'                    => 'Predis\Command\ListRange',
            'ltrim'                     => 'Predis\Command\ListTrim',
            'lindex'                    => 'Predis\Command\ListIndex',
            'lset'                      => 'Predis\Command\ListSet',
            'lrem'                      => 'Predis\Command\ListRemove',
            'lpop'                      => 'Predis\Command\ListPopFirst',
            'rpop'                      => 'Predis\Command\ListPopLast',
            'rpoplpush'                 => 'Predis\Command\ListPopLastPushHead',

            /* commands operating on sets */
            'sadd'                      => 'Predis\Command\SetAdd',
            'srem'                      => 'Predis\Command\SetRemove',
            'spop'                      => 'Predis\Command\SetPop',
            'smove'                     => 'Predis\Command\SetMove',
            'scard'                     => 'Predis\Command\SetCardinality',
            'sismember'                 => 'Predis\Command\SetIsMember',
            'sinter'                    => 'Predis\Command\SetIntersection',
            'sinterstore'               => 'Predis\Command\SetIntersectionStore',
            'sunion'                    => 'Predis\Command\SetUnion',
            'sunionstore'               => 'Predis\Command\SetUnionStore',
            'sdiff'                     => 'Predis\Command\SetDifference',
            'sdiffstore'                => 'Predis\Command\SetDifferenceStore',
            'smembers'                  => 'Predis\Command\SetMembers',
            'srandmember'               => 'Predis\Command\SetRandomMember',

            /* commands operating on sorted sets */
            'zadd'                      => 'Predis\Command\ZSetAdd',
            'zincrby'                   => 'Predis\Command\ZSetIncrementBy',
            'zrem'                      => 'Predis\Command\ZSetRemove',
            'zrange'                    => 'Predis\Command\ZSetRange',
            'zrevrange'                 => 'Predis\Command\ZSetReverseRange',
            'zrangebyscore'             => 'Predis\Command\ZSetRangeByScore',
            'zcard'                     => 'Predis\Command\ZSetCardinality',
            'zscore'                    => 'Predis\Command\ZSetScore',
            'zremrangebyscore'          => 'Predis\Command\ZSetRemoveRangeByScore',

            /* connection related commands */
            'ping'                      => 'Predis\Command\ConnectionPing',
            'auth'                      => 'Predis\Command\ConnectionAuth',
            'select'                    => 'Predis\Command\ConnectionSelect',
            'echo'                      => 'Predis\Command\ConnectionEcho',
            'quit'                      => 'Predis\Command\ConnectionQuit',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfo',
            'slaveof'                   => 'Predis\Command\ServerSlaveOf',
            'monitor'                   => 'Predis\Command\ServerMonitor',
            'dbsize'                    => 'Predis\Command\ServerDatabaseSize',
            'flushdb'                   => 'Predis\Command\ServerFlushDatabase',
            'flushall'                  => 'Predis\Command\ServerFlushAll',
            'save'                      => 'Predis\Command\ServerSave',
            'bgsave'                    => 'Predis\Command\ServerBackgroundSave',
            'lastsave'                  => 'Predis\Command\ServerLastSave',
            'shutdown'                  => 'Predis\Command\ServerShutdown',
            'bgrewriteaof'              => 'Predis\Command\ServerBackgroundRewriteAOF',


            /* ---------------- Redis 2.0 ---------------- */

            /* commands operating on string values */
            'setex'                     => 'Predis\Command\StringSetExpire',
            'append'                    => 'Predis\Command\StringAppend',
            'substr'                    => 'Predis\Command\StringSubstr',

            /* commands operating on lists */
            'blpop'                     => 'Predis\Command\ListPopFirstBlocking',
            'brpop'                     => 'Predis\Command\ListPopLastBlocking',

            /* commands operating on sorted sets */
            'zunionstore'               => 'Predis\Command\ZSetUnionStore',
            'zinterstore'               => 'Predis\Command\ZSetIntersectionStore',
            'zcount'                    => 'Predis\Command\ZSetCount',
            'zrank'                     => 'Predis\Command\ZSetRank',
            'zrevrank'                  => 'Predis\Command\ZSetReverseRank',
            'zremrangebyrank'           => 'Predis\Command\ZSetRemoveRangeByRank',

            /* commands operating on hashes */
            'hset'                      => 'Predis\Command\HashSet',
            'hsetnx'                    => 'Predis\Command\HashSetPreserve',
            'hmset'                     => 'Predis\Command\HashSetMultiple',
            'hincrby'                   => 'Predis\Command\HashIncrementBy',
            'hget'                      => 'Predis\Command\HashGet',
            'hmget'                     => 'Predis\Command\HashGetMultiple',
            'hdel'                      => 'Predis\Command\HashDelete',
            'hexists'                   => 'Predis\Command\HashExists',
            'hlen'                      => 'Predis\Command\HashLength',
            'hkeys'                     => 'Predis\Command\HashKeys',
            'hvals'                     => 'Predis\Command\HashValues',
            'hgetall'                   => 'Predis\Command\HashGetAll',

            /* transactions */
            'multi'                     => 'Predis\Command\TransactionMulti',
            'exec'                      => 'Predis\Command\TransactionExec',
            'discard'                   => 'Predis\Command\TransactionDiscard',

            /* publish - subscribe */
            'subscribe'                 => 'Predis\Command\PubSubSubscribe',
            'unsubscribe'               => 'Predis\Command\PubSubUnsubscribe',
            'psubscribe'                => 'Predis\Command\PubSubSubscribeByPattern',
            'punsubscribe'              => 'Predis\Command\PubSubUnsubscribeByPattern',
            'publish'                   => 'Predis\Command\PubSubPublish',

            /* remote server control commands */
            'config'                    => 'Predis\Command\ServerConfig',
        );
    }
}

/**
 * Server profile for Redis v1.2.x.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersion12 extends ServerProfile
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '1.2';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array(
            /* ---------------- Redis 1.2 ---------------- */

            /* commands operating on the key space */
            'exists'                    => 'Predis\Command\KeyExists',
            'del'                       => 'Predis\Command\KeyDelete',
            'type'                      => 'Predis\Command\KeyType',
            'keys'                      => 'Predis\Command\KeyKeysV12x',
            'randomkey'                 => 'Predis\Command\KeyRandom',
            'rename'                    => 'Predis\Command\KeyRename',
            'renamenx'                  => 'Predis\Command\KeyRenamePreserve',
            'expire'                    => 'Predis\Command\KeyExpire',
            'expireat'                  => 'Predis\Command\KeyExpireAt',
            'ttl'                       => 'Predis\Command\KeyTimeToLive',
            'move'                      => 'Predis\Command\KeyMove',
            'sort'                      => 'Predis\Command\KeySort',

            /* commands operating on string values */
            'set'                       => 'Predis\Command\StringSet',
            'setnx'                     => 'Predis\Command\StringSetPreserve',
            'mset'                      => 'Predis\Command\StringSetMultiple',
            'msetnx'                    => 'Predis\Command\StringSetMultiplePreserve',
            'get'                       => 'Predis\Command\StringGet',
            'mget'                      => 'Predis\Command\StringGetMultiple',
            'getset'                    => 'Predis\Command\StringGetSet',
            'incr'                      => 'Predis\Command\StringIncrement',
            'incrby'                    => 'Predis\Command\StringIncrementBy',
            'decr'                      => 'Predis\Command\StringDecrement',
            'decrby'                    => 'Predis\Command\StringDecrementBy',

            /* commands operating on lists */
            'rpush'                     => 'Predis\Command\ListPushTail',
            'lpush'                     => 'Predis\Command\ListPushHead',
            'llen'                      => 'Predis\Command\ListLength',
            'lrange'                    => 'Predis\Command\ListRange',
            'ltrim'                     => 'Predis\Command\ListTrim',
            'lindex'                    => 'Predis\Command\ListIndex',
            'lset'                      => 'Predis\Command\ListSet',
            'lrem'                      => 'Predis\Command\ListRemove',
            'lpop'                      => 'Predis\Command\ListPopFirst',
            'rpop'                      => 'Predis\Command\ListPopLast',
            'rpoplpush'                 => 'Predis\Command\ListPopLastPushHead',

            /* commands operating on sets */
            'sadd'                      => 'Predis\Command\SetAdd',
            'srem'                      => 'Predis\Command\SetRemove',
            'spop'                      => 'Predis\Command\SetPop',
            'smove'                     => 'Predis\Command\SetMove',
            'scard'                     => 'Predis\Command\SetCardinality',
            'sismember'                 => 'Predis\Command\SetIsMember',
            'sinter'                    => 'Predis\Command\SetIntersection',
            'sinterstore'               => 'Predis\Command\SetIntersectionStore',
            'sunion'                    => 'Predis\Command\SetUnion',
            'sunionstore'               => 'Predis\Command\SetUnionStore',
            'sdiff'                     => 'Predis\Command\SetDifference',
            'sdiffstore'                => 'Predis\Command\SetDifferenceStore',
            'smembers'                  => 'Predis\Command\SetMembers',
            'srandmember'               => 'Predis\Command\SetRandomMember',

            /* commands operating on sorted sets */
            'zadd'                      => 'Predis\Command\ZSetAdd',
            'zincrby'                   => 'Predis\Command\ZSetIncrementBy',
            'zrem'                      => 'Predis\Command\ZSetRemove',
            'zrange'                    => 'Predis\Command\ZSetRange',
            'zrevrange'                 => 'Predis\Command\ZSetReverseRange',
            'zrangebyscore'             => 'Predis\Command\ZSetRangeByScore',
            'zcard'                     => 'Predis\Command\ZSetCardinality',
            'zscore'                    => 'Predis\Command\ZSetScore',
            'zremrangebyscore'          => 'Predis\Command\ZSetRemoveRangeByScore',

            /* connection related commands */
            'ping'                      => 'Predis\Command\ConnectionPing',
            'auth'                      => 'Predis\Command\ConnectionAuth',
            'select'                    => 'Predis\Command\ConnectionSelect',
            'echo'                      => 'Predis\Command\ConnectionEcho',
            'quit'                      => 'Predis\Command\ConnectionQuit',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfo',
            'slaveof'                   => 'Predis\Command\ServerSlaveOf',
            'monitor'                   => 'Predis\Command\ServerMonitor',
            'dbsize'                    => 'Predis\Command\ServerDatabaseSize',
            'flushdb'                   => 'Predis\Command\ServerFlushDatabase',
            'flushall'                  => 'Predis\Command\ServerFlushAll',
            'save'                      => 'Predis\Command\ServerSave',
            'bgsave'                    => 'Predis\Command\ServerBackgroundSave',
            'lastsave'                  => 'Predis\Command\ServerLastSave',
            'shutdown'                  => 'Predis\Command\ServerShutdown',
            'bgrewriteaof'              => 'Predis\Command\ServerBackgroundRewriteAOF',
        );
    }
}

/**
 * Server profile for Redis v2.2.x.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ServerVersion22 extends ServerProfile
{
    /**
     * {@inheritdoc}
     */
    public function getVersion()
    {
        return '2.2';
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedCommands()
    {
        return array(
            /* ---------------- Redis 1.2 ---------------- */

            /* commands operating on the key space */
            'exists'                    => 'Predis\Command\KeyExists',
            'del'                       => 'Predis\Command\KeyDelete',
            'type'                      => 'Predis\Command\KeyType',
            'keys'                      => 'Predis\Command\KeyKeys',
            'randomkey'                 => 'Predis\Command\KeyRandom',
            'rename'                    => 'Predis\Command\KeyRename',
            'renamenx'                  => 'Predis\Command\KeyRenamePreserve',
            'expire'                    => 'Predis\Command\KeyExpire',
            'expireat'                  => 'Predis\Command\KeyExpireAt',
            'ttl'                       => 'Predis\Command\KeyTimeToLive',
            'move'                      => 'Predis\Command\KeyMove',
            'sort'                      => 'Predis\Command\KeySort',

            /* commands operating on string values */
            'set'                       => 'Predis\Command\StringSet',
            'setnx'                     => 'Predis\Command\StringSetPreserve',
            'mset'                      => 'Predis\Command\StringSetMultiple',
            'msetnx'                    => 'Predis\Command\StringSetMultiplePreserve',
            'get'                       => 'Predis\Command\StringGet',
            'mget'                      => 'Predis\Command\StringGetMultiple',
            'getset'                    => 'Predis\Command\StringGetSet',
            'incr'                      => 'Predis\Command\StringIncrement',
            'incrby'                    => 'Predis\Command\StringIncrementBy',
            'decr'                      => 'Predis\Command\StringDecrement',
            'decrby'                    => 'Predis\Command\StringDecrementBy',

            /* commands operating on lists */
            'rpush'                     => 'Predis\Command\ListPushTail',
            'lpush'                     => 'Predis\Command\ListPushHead',
            'llen'                      => 'Predis\Command\ListLength',
            'lrange'                    => 'Predis\Command\ListRange',
            'ltrim'                     => 'Predis\Command\ListTrim',
            'lindex'                    => 'Predis\Command\ListIndex',
            'lset'                      => 'Predis\Command\ListSet',
            'lrem'                      => 'Predis\Command\ListRemove',
            'lpop'                      => 'Predis\Command\ListPopFirst',
            'rpop'                      => 'Predis\Command\ListPopLast',
            'rpoplpush'                 => 'Predis\Command\ListPopLastPushHead',

            /* commands operating on sets */
            'sadd'                      => 'Predis\Command\SetAdd',
            'srem'                      => 'Predis\Command\SetRemove',
            'spop'                      => 'Predis\Command\SetPop',
            'smove'                     => 'Predis\Command\SetMove',
            'scard'                     => 'Predis\Command\SetCardinality',
            'sismember'                 => 'Predis\Command\SetIsMember',
            'sinter'                    => 'Predis\Command\SetIntersection',
            'sinterstore'               => 'Predis\Command\SetIntersectionStore',
            'sunion'                    => 'Predis\Command\SetUnion',
            'sunionstore'               => 'Predis\Command\SetUnionStore',
            'sdiff'                     => 'Predis\Command\SetDifference',
            'sdiffstore'                => 'Predis\Command\SetDifferenceStore',
            'smembers'                  => 'Predis\Command\SetMembers',
            'srandmember'               => 'Predis\Command\SetRandomMember',

            /* commands operating on sorted sets */
            'zadd'                      => 'Predis\Command\ZSetAdd',
            'zincrby'                   => 'Predis\Command\ZSetIncrementBy',
            'zrem'                      => 'Predis\Command\ZSetRemove',
            'zrange'                    => 'Predis\Command\ZSetRange',
            'zrevrange'                 => 'Predis\Command\ZSetReverseRange',
            'zrangebyscore'             => 'Predis\Command\ZSetRangeByScore',
            'zcard'                     => 'Predis\Command\ZSetCardinality',
            'zscore'                    => 'Predis\Command\ZSetScore',
            'zremrangebyscore'          => 'Predis\Command\ZSetRemoveRangeByScore',

            /* connection related commands */
            'ping'                      => 'Predis\Command\ConnectionPing',
            'auth'                      => 'Predis\Command\ConnectionAuth',
            'select'                    => 'Predis\Command\ConnectionSelect',
            'echo'                      => 'Predis\Command\ConnectionEcho',
            'quit'                      => 'Predis\Command\ConnectionQuit',

            /* remote server control commands */
            'info'                      => 'Predis\Command\ServerInfo',
            'slaveof'                   => 'Predis\Command\ServerSlaveOf',
            'monitor'                   => 'Predis\Command\ServerMonitor',
            'dbsize'                    => 'Predis\Command\ServerDatabaseSize',
            'flushdb'                   => 'Predis\Command\ServerFlushDatabase',
            'flushall'                  => 'Predis\Command\ServerFlushAll',
            'save'                      => 'Predis\Command\ServerSave',
            'bgsave'                    => 'Predis\Command\ServerBackgroundSave',
            'lastsave'                  => 'Predis\Command\ServerLastSave',
            'shutdown'                  => 'Predis\Command\ServerShutdown',
            'bgrewriteaof'              => 'Predis\Command\ServerBackgroundRewriteAOF',


            /* ---------------- Redis 2.0 ---------------- */

            /* commands operating on string values */
            'setex'                     => 'Predis\Command\StringSetExpire',
            'append'                    => 'Predis\Command\StringAppend',
            'substr'                    => 'Predis\Command\StringSubstr',

            /* commands operating on lists */
            'blpop'                     => 'Predis\Command\ListPopFirstBlocking',
            'brpop'                     => 'Predis\Command\ListPopLastBlocking',

            /* commands operating on sorted sets */
            'zunionstore'               => 'Predis\Command\ZSetUnionStore',
            'zinterstore'               => 'Predis\Command\ZSetIntersectionStore',
            'zcount'                    => 'Predis\Command\ZSetCount',
            'zrank'                     => 'Predis\Command\ZSetRank',
            'zrevrank'                  => 'Predis\Command\ZSetReverseRank',
            'zremrangebyrank'           => 'Predis\Command\ZSetRemoveRangeByRank',

            /* commands operating on hashes */
            'hset'                      => 'Predis\Command\HashSet',
            'hsetnx'                    => 'Predis\Command\HashSetPreserve',
            'hmset'                     => 'Predis\Command\HashSetMultiple',
            'hincrby'                   => 'Predis\Command\HashIncrementBy',
            'hget'                      => 'Predis\Command\HashGet',
            'hmget'                     => 'Predis\Command\HashGetMultiple',
            'hdel'                      => 'Predis\Command\HashDelete',
            'hexists'                   => 'Predis\Command\HashExists',
            'hlen'                      => 'Predis\Command\HashLength',
            'hkeys'                     => 'Predis\Command\HashKeys',
            'hvals'                     => 'Predis\Command\HashValues',
            'hgetall'                   => 'Predis\Command\HashGetAll',

            /* transactions */
            'multi'                     => 'Predis\Command\TransactionMulti',
            'exec'                      => 'Predis\Command\TransactionExec',
            'discard'                   => 'Predis\Command\TransactionDiscard',

            /* publish - subscribe */
            'subscribe'                 => 'Predis\Command\PubSubSubscribe',
            'unsubscribe'               => 'Predis\Command\PubSubUnsubscribe',
            'psubscribe'                => 'Predis\Command\PubSubSubscribeByPattern',
            'punsubscribe'              => 'Predis\Command\PubSubUnsubscribeByPattern',
            'publish'                   => 'Predis\Command\PubSubPublish',

            /* remote server control commands */
            'config'                    => 'Predis\Command\ServerConfig',


            /* ---------------- Redis 2.2 ---------------- */

            /* commands operating on the key space */
            'persist'                   => 'Predis\Command\KeyPersist',

            /* commands operating on string values */
            'strlen'                    => 'Predis\Command\StringStrlen',
            'setrange'                  => 'Predis\Command\StringSetRange',
            'getrange'                  => 'Predis\Command\StringGetRange',
            'setbit'                    => 'Predis\Command\StringSetBit',
            'getbit'                    => 'Predis\Command\StringGetBit',

            /* commands operating on lists */
            'rpushx'                    => 'Predis\Command\ListPushTailX',
            'lpushx'                    => 'Predis\Command\ListPushHeadX',
            'linsert'                   => 'Predis\Command\ListInsert',
            'brpoplpush'                => 'Predis\Command\ListPopLastPushHeadBlocking',

            /* commands operating on sorted sets */
            'zrevrangebyscore'          => 'Predis\Command\ZSetReverseRangeByScore',

            /* transactions */
            'watch'                     => 'Predis\Command\TransactionWatch',
            'unwatch'                   => 'Predis\Command\TransactionUnwatch',

            /* remote server control commands */
            'object'                    => 'Predis\Command\ServerObject',
            'slowlog'                   => 'Predis\Command\ServerSlowlog',
        );
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Protocol;

use Predis\Command\CommandInterface;
use Predis\CommunicationException;
use Predis\Connection\ComposableConnectionInterface;

/**
 * Interface that defines an handler able to parse a reply.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ResponseHandlerInterface
{
    /**
     * Parses a type of reply returned by Redis and reads more data from the
     * connection if needed.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param string $payload Initial payload of the reply.
     * @return mixed
     */
    function handle(ComposableConnectionInterface $connection, $payload);
}

/**
 * Interface that defines a response reader able to parse replies returned by
 * Redis and deserialize them to PHP objects.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ResponseReaderInterface
{
    /**
     * Reads replies from a connection to Redis and deserializes them.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @return mixed
     */
    public function read(ComposableConnectionInterface $connection);
}

/**
 * Interface that defines a protocol processor that serializes Redis commands
 * and parses replies returned by the server to PHP objects.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ProtocolInterface extends ResponseReaderInterface
{
    /**
     * Writes a Redis command on the specified connection.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param CommandInterface $command Redis command.
     */
    public function write(ComposableConnectionInterface $connection, CommandInterface $command);

    /**
     * Sets the options for the protocol processor.
     *
     * @param string $option Name of the option.
     * @param mixed $value Value of the option.
     */
    public function setOption($option, $value);
}

/**
 * Interface that defines a custom serializer for Redis commands.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandSerializerInterface
{
    /**
     * Serializes a Redis command.
     *
     * @param CommandInterface $command Redis command.
     * @return string
     */
    public function serialize(CommandInterface $command);
}

/**
 * Interface that defines a customizable protocol processor that serializes
 * Redis commands and parses replies returned by the server to PHP objects
 * using a pluggable set of classes defining the underlying wire protocol.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface ComposableProtocolInterface extends ProtocolInterface
{
    /**
     * Sets the command serializer to be used by the protocol processor.
     *
     * @param CommandSerializerInterface $serializer Command serializer.
     */
    public function setSerializer(CommandSerializerInterface $serializer);

    /**
     * Returns the command serializer used by the protocol processor.
     *
     * @return CommandSerializerInterface
     */
    public function getSerializer();

    /**
     * Sets the response reader to be used by the protocol processor.
     *
     * @param ResponseReaderInterface $reader Response reader.
     */
    public function setReader(ResponseReaderInterface $reader);

    /**
     * Returns the response reader used by the protocol processor.
     *
     * @return ResponseReaderInterface
     */
    public function getReader();
}

/**
 * Exception class that identifies errors encountered while
 * handling the Redis wire protocol.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ProtocolException extends CommunicationException
{
}

/* --------------------------------------------------------------------------- */

namespace Predis\Command\Processor;

use Predis\Command\CommandInterface;
use Predis\Command\PrefixableCommandInterface;

/**
 * Defines an object that can process commands using command processors.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandProcessingInterface
{
    /**
     * Associates a command processor.
     *
     * @param CommandProcessorInterface $processor The command processor.
     */
    public function setProcessor(CommandProcessorInterface $processor);

    /**
     * Returns the associated command processor.
     *
     * @return CommandProcessorInterface
     */
    public function getProcessor();
}

/**
 * A command processor processes commands before they are sent to Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandProcessorInterface
{
    /**
     * Processes a Redis command.
     *
     * @param CommandInterface $command Redis command.
     */
    public function process(CommandInterface $command);
}

/**
 * A command processor chain processes a command using multiple chained command
 * processor before it is sent to Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandProcessorChainInterface extends CommandProcessorInterface, \IteratorAggregate, \Countable
{
    /**
     * Adds a command processor.
     *
     * @param CommandProcessorInterface $processor A command processor.
     */
    public function add(CommandProcessorInterface $processor);

    /**
     * Removes a command processor from the chain.
     *
     * @param CommandProcessorInterface $processor A command processor.
     */
    public function remove(CommandProcessorInterface $processor);

    /**
     * Returns an ordered list of the command processors in the chain.
     *
     * @return array
     */
    public function getProcessors();
}

/**
 * Default implementation of a command processors chain.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ProcessorChain implements CommandProcessorChainInterface, \ArrayAccess
{
    private $processors = array();

    /**
     * @param array $processors List of instances of CommandProcessorInterface.
     */
    public function __construct($processors = array())
    {
        foreach ($processors as $processor) {
            $this->add($processor);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function add(CommandProcessorInterface $processor)
    {
        $this->processors[] = $processor;
    }

    /**
     * {@inheritdoc}
     */
    public function remove(CommandProcessorInterface $processor)
    {
        if (false !== $index = array_search($processor, $this->processors, true)) {
            unset($this[$index]);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function process(CommandInterface $command)
    {
        for ($i = 0; $i < $count = count($this->processors); $i++) {
            $this->processors[$i]->process($command);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getProcessors()
    {
        return $this->processors;
    }

    /**
     * Returns an iterator over the list of command processor in the chain.
     *
     * @return \ArrayIterator
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->processors);
    }

    /**
     * Returns the number of command processors in the chain.
     *
     * @return int
     */
    public function count()
    {
        return count($this->processors);
    }

    /**
     * {@inheritdoc}
     */
    public function offsetExists($index)
    {
        return isset($this->processors[$index]);
    }

    /**
     * {@inheritdoc}
     */
    public function offsetGet($index)
    {
        return $this->processors[$index];
    }

    /**
     * {@inheritdoc}
     */
    public function offsetSet($index, $processor)
    {
        if (!$processor instanceof CommandProcessorInterface) {
            throw new \InvalidArgumentException(
                'A processor chain can hold only instances of classes implementing '.
                'the Predis\Command\Processor\CommandProcessorInterface interface'
            );
        }

        $this->processors[$index] = $processor;
    }

    /**
     * {@inheritdoc}
     */
    public function offsetUnset($index)
    {
        unset($this->processors[$index]);
        $this->processors = array_values($this->processors);
    }
}

/**
 * Command processor that is used to prefix the keys contained in the arguments
 * of a Redis command.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class KeyPrefixProcessor implements CommandProcessorInterface
{
    private $prefix;

    /**
     * @param string $prefix Prefix for the keys.
     */
    public function __construct($prefix)
    {
        $this->setPrefix($prefix);
    }

    /**
     * Sets a prefix that is applied to all the keys.
     *
     * @param string $prefix Prefix for the keys.
     */
    public function setPrefix($prefix)
    {
        $this->prefix = $prefix;
    }

    /**
     * Gets the current prefix.
     *
     * @return string
     */
    public function getPrefix()
    {
        return $this->prefix;
    }

    /**
     * {@inheritdoc}
     */
    public function process(CommandInterface $command)
    {
        if ($command instanceof PrefixableCommandInterface && $command->getArguments()) {
            $command->prefixKeys($this->prefix);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function __toString()
    {
        return $this->getPrefix();
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Pipeline;

use SplQueue;
use Predis\Connection\ConnectionInterface;
use Predis\Connection\ReplicationConnectionInterface;
use Iterator;
use Predis\ClientException;
use Predis\ResponseErrorInterface;
use Predis\ResponseObjectInterface;
use Predis\ServerException;
use Predis\Connection\SingleConnectionInterface;
use Predis\Profile\ServerProfile;
use Predis\Profile\ServerProfileInterface;
use Predis\BasicClientInterface;
use Predis\ClientInterface;
use Predis\ExecutableContextInterface;
use Predis\Command\CommandInterface;
use Predis\CommunicationException;

/**
 * Defines a strategy to write a list of commands to the network
 * and read back their replies.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface PipelineExecutorInterface
{
    /**
     * Writes a list of commands to the network and reads back their replies.
     *
     * @param ConnectionInterface $connection Connection to Redis.
     * @param SplQueue $commands Commands queued for execution.
     * @return array
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands);
}

/**
 * Implements a pipeline executor strategy that does not fail when an error is
 * encountered, but adds the returned error in the replies array.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SafeExecutor implements PipelineExecutorInterface
{
    /**
     * {@inheritdoc}
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands)
    {
        $size = count($commands);
        $values = array();

        foreach ($commands as $command) {
            try {
                $connection->writeCommand($command);
            } catch (CommunicationException $exception) {
                return array_fill(0, $size, $exception);
            }
        }

        for ($i = 0; $i < $size; $i++) {
            $command = $commands->dequeue();

            try {
                $response = $connection->readResponse($command);
                $values[$i] = $response instanceof \Iterator ? iterator_to_array($response) : $response;
            } catch (CommunicationException $exception) {
                $toAdd = count($commands) - count($values);
                $values = array_merge($values, array_fill(0, $toAdd, $exception));
                break;
            }
        }

        return $values;
    }
}

/**
 * Implements the standard pipeline executor strategy used
 * to write a list of commands and read their replies over
 * a connection to Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class StandardExecutor implements PipelineExecutorInterface
{
    protected $exceptions;

    /**
     * @param bool $exceptions Specifies if the executor should throw exceptions on server errors.
     */
    public function __construct($exceptions = true)
    {
        $this->exceptions = (bool) $exceptions;
    }

    /**
     * Allows the pipeline executor to perform operations on the
     * connection before starting to execute the commands stored
     * in the pipeline.
     *
     * @param ConnectionInterface $connection Connection instance.
     */
    protected function checkConnection(ConnectionInterface $connection)
    {
        if ($connection instanceof ReplicationConnectionInterface) {
            $connection->switchTo('master');
        }
    }

    /**
     * Handles a response object.
     *
     * @param ConnectionInterface $connection
     * @param CommandInterface $command
     * @param ResponseObjectInterface $response
     * @return mixed
     */
    protected function onResponseObject(ConnectionInterface $connection, CommandInterface $command, ResponseObjectInterface $response)
    {
        if ($response instanceof ResponseErrorInterface) {
            return $this->onResponseError($connection, $response);
        }

        if ($response instanceof Iterator) {
            return $command->parseResponse(iterator_to_array($response));
        }

        return $response;
    }

    /**
     * Handles -ERR responses returned by Redis.
     *
     * @param ConnectionInterface $connection The connection that returned the error.
     * @param ResponseErrorInterface $response The error response instance.
     */
    protected function onResponseError(ConnectionInterface $connection, ResponseErrorInterface $response)
    {
        if (!$this->exceptions) {
            return $response;
        }

        // Force disconnection to prevent protocol desynchronization.
        $connection->disconnect();
        $message = $response->getMessage();

        throw new ServerException($message);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands)
    {
        $this->checkConnection($connection);

        foreach ($commands as $command) {
            $connection->writeCommand($command);
        }

        $values = array();

        while (!$commands->isEmpty()) {
            $command = $commands->dequeue();
            $response = $connection->readResponse($command);

            if ($response instanceof ResponseObjectInterface) {
                $values[] = $this->onResponseObject($connection, $command, $response);
            } else {
                $values[] = $command->parseResponse($response);
            }
        }

        return $values;
    }
}

/**
 * Implements a pipeline executor strategy for connection clusters that does
 * not fail when an error is encountered, but adds the returned error in the
 * replies array.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SafeClusterExecutor implements PipelineExecutorInterface
{
    /**
     * {@inheritdoc}
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands)
    {
        $size = count($commands);
        $values = array();
        $connectionExceptions = array();

        foreach ($commands as $command) {
            $cmdConnection = $connection->getConnection($command);

            if (isset($connectionExceptions[spl_object_hash($cmdConnection)])) {
                continue;
            }

            try {
                $cmdConnection->writeCommand($command);
            } catch (CommunicationException $exception) {
                $connectionExceptions[spl_object_hash($cmdConnection)] = $exception;
            }
        }

        for ($i = 0; $i < $size; $i++) {
            $command = $commands->dequeue();

            $cmdConnection = $connection->getConnection($command);
            $connectionObjectHash = spl_object_hash($cmdConnection);

            if (isset($connectionExceptions[$connectionObjectHash])) {
                $values[$i] = $connectionExceptions[$connectionObjectHash];
                continue;
            }

            try {
                $response = $cmdConnection->readResponse($command);
                $values[$i] = $response instanceof \Iterator ? iterator_to_array($response) : $response;
            } catch (CommunicationException $exception) {
                $values[$i] = $exception;
                $connectionExceptions[$connectionObjectHash] = $exception;
            }
        }

        return $values;
    }
}

/**
 * Implements a pipeline executor strategy that writes a list of commands to
 * the connection object but does not read back their replies.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class FireAndForgetExecutor implements PipelineExecutorInterface
{
    /**
     * Allows the pipeline executor to perform operations on the
     * connection before starting to execute the commands stored
     * in the pipeline.
     *
     * @param ConnectionInterface $connection Connection instance.
     */
    protected function checkConnection(ConnectionInterface $connection)
    {
        if ($connection instanceof ReplicationConnectionInterface) {
            $connection->switchTo('master');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands)
    {
        $this->checkConnection($connection);

        while (!$commands->isEmpty()) {
            $connection->writeCommand($commands->dequeue());
        }

        $connection->disconnect();

        return array();
    }
}

/**
 * Abstraction of a pipeline context where write and read operations
 * of commands and their replies over the network are pipelined.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PipelineContext implements BasicClientInterface, ExecutableContextInterface
{
    private $client;
    private $executor;
    private $pipeline;

    private $replies = array();
    private $running = false;

    /**
     * @param ClientInterface $client Client instance used by the context.
     * @param PipelineExecutorInterface $executor Pipeline executor instace.
     */
    public function __construct(ClientInterface $client, PipelineExecutorInterface $executor = null)
    {
        $this->client = $client;
        $this->executor = $executor ?: $this->createExecutor($client);
        $this->pipeline = new SplQueue();
    }

    /**
     * Returns a pipeline executor depending on the kind of the underlying
     * connection and the passed options.
     *
     * @param ClientInterface $client Client instance used by the context.
     * @return PipelineExecutorInterface
     */
    protected function createExecutor(ClientInterface $client)
    {
        $options = $client->getOptions();

        if (isset($options->exceptions)) {
            return new StandardExecutor($options->exceptions);
        }

        return new StandardExecutor();
    }

    /**
     * Queues a command into the pipeline buffer.
     *
     * @param string $method Command ID.
     * @param array $arguments Arguments for the command.
     * @return PipelineContext
     */
    public function __call($method, $arguments)
    {
        $command = $this->client->createCommand($method, $arguments);
        $this->recordCommand($command);

        return $this;
    }

    /**
     * Queues a command instance into the pipeline buffer.
     *
     * @param CommandInterface $command Command to queue in the buffer.
     */
    protected function recordCommand(CommandInterface $command)
    {
        $this->pipeline->enqueue($command);
    }

    /**
     * Queues a command instance into the pipeline buffer.
     *
     * @param CommandInterface $command Command to queue in the buffer.
     */
    public function executeCommand(CommandInterface $command)
    {
        $this->recordCommand($command);
    }

    /**
     * Flushes the buffer that holds the queued commands.
     *
     * @param Boolean $send Specifies if the commands in the buffer should be sent to Redis.
     * @return PipelineContext
     */
    public function flushPipeline($send = true)
    {
        if ($send && !$this->pipeline->isEmpty()) {
            $connection = $this->client->getConnection();
            $replies = $this->executor->execute($connection, $this->pipeline);
            $this->replies = array_merge($this->replies, $replies);
        } else {
            $this->pipeline = new SplQueue();
        }

        return $this;
    }

    /**
     * Marks the running status of the pipeline.
     *
     * @param Boolean $bool True if the pipeline is running.
     *                      False if the pipeline is not running.
     */
    private function setRunning($bool)
    {
        if ($bool === true && $this->running === true) {
            throw new ClientException("This pipeline is already opened");
        }

        $this->running = $bool;
    }

    /**
     * Handles the actual execution of the whole pipeline.
     *
     * @param mixed $callable Optional callback for execution.
     * @return array
     */
    public function execute($callable = null)
    {
        if ($callable && !is_callable($callable)) {
            throw new \InvalidArgumentException('Argument passed must be a callable object');
        }

        $this->setRunning(true);
        $pipelineBlockException = null;

        try {
            if ($callable !== null) {
                call_user_func($callable, $this);
            }
            $this->flushPipeline();
        } catch (\Exception $exception) {
            $pipelineBlockException = $exception;
        }

        $this->setRunning(false);

        if ($pipelineBlockException !== null) {
            throw $pipelineBlockException;
        }

        return $this->replies;
    }

    /**
     * Returns the underlying client instance used by the pipeline object.
     *
     * @return ClientInterface
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Returns the underlying pipeline executor used by the pipeline object.
     *
     * @return PipelineExecutorInterface
     */
    public function getExecutor()
    {
        return $this->executor;
    }
}

/**
 * Implements a pipeline executor that wraps the whole pipeline
 * in a MULTI / EXEC context to make sure that it is executed
 * correctly.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MultiExecExecutor implements PipelineExecutorInterface
{
    protected $profile;

    /**
     *
     */
    public function __construct(ServerProfileInterface $profile = null)
    {
        $this->setProfile($profile ?: ServerProfile::getDefault());
    }

    /**
     * Allows the pipeline executor to perform operations on the
     * connection before starting to execute the commands stored
     * in the pipeline.
     *
     * @param ConnectionInterface $connection Connection instance.
     */
    protected function checkConnection(ConnectionInterface $connection)
    {
        if (!$connection instanceof SingleConnectionInterface) {
            $class = __CLASS__;
            throw new ClientException("$class can be used only with single connections");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function execute(ConnectionInterface $connection, SplQueue $commands)
    {
        $this->checkConnection($connection);

        $cmd = $this->profile->createCommand('multi');
        $connection->executeCommand($cmd);

        foreach ($commands as $command) {
            $connection->writeCommand($command);
        }

        foreach ($commands as $command) {
            $response = $connection->readResponse($command);

            if ($response instanceof ResponseErrorInterface) {
                $cmd = $this->profile->createCommand('discard');
                $connection->executeCommand($cmd);

                throw new ServerException($response->getMessage());
            }
        }

        $cmd = $this->profile->createCommand('exec');
        $responses = $connection->executeCommand($cmd);

        if (!isset($responses)) {
            throw new ClientException('The underlying transaction has been aborted by the server');
        }

        if (count($responses) !== count($commands)) {
            throw new ClientException("Invalid number of replies [expected: ".count($commands)." - actual: ".count($responses)."]");
        }

        $consumer = $responses instanceof Iterator ? 'consumeIteratorResponse' : 'consumeArrayResponse';

        return $this->$consumer($commands, $responses);
    }

    /**
     * Consumes an iterator response returned by EXEC.
     *
     * @param SplQueue $commands Pipelined commands
     * @param Iterator $responses Responses returned by EXEC.
     * @return array
     */
    protected function consumeIteratorResponse(SplQueue $commands, Iterator $responses)
    {
        $values = array();

        foreach ($responses as $response) {
            $command = $commands->dequeue();

            if ($response instanceof ResponseObjectInterface) {
                if ($response instanceof Iterator) {
                    $response = iterator_to_array($response);
                    $values[] = $command->parseResponse($response);
                } else {
                    $values[] = $response;
                }
            } else {
                $values[] = $command->parseResponse($response);
            }
        }

        return $values;
    }

    /**
     * Consumes an array response returned by EXEC.
     *
     * @param SplQueue $commands Pipelined commands
     * @param Array $responses Responses returned by EXEC.
     * @return array
     */
    protected function consumeArrayResponse(SplQueue $commands, Array &$responses)
    {
        $size = count($commands);
        $values = array();

        for ($i = 0; $i < $size; $i++) {
            $command = $commands->dequeue();
            $response = $responses[$i];

            if ($response instanceof ResponseObjectInterface) {
                $values[$i] = $response;
            } else {
                $values[$i] = $command->parseResponse($response);
            }

            unset($responses[$i]);
        }

        return $values;
    }

    /**
     * @param ServerProfileInterface $profile Server profile.
     */
    public function setProfile(ServerProfileInterface $profile)
    {
        if (!$profile->supportsCommands(array('multi', 'exec', 'discard'))) {
            throw new ClientException('The specified server profile must support MULTI, EXEC and DISCARD.');
        }

        $this->profile = $profile;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Protocol\Text;

use Predis\Command\CommandInterface;
use Predis\Connection\ComposableConnectionInterface;
use Predis\Protocol\ResponseReaderInterface;
use Predis\Protocol\CommandSerializerInterface;
use Predis\Protocol\ComposableProtocolInterface;
use Predis\CommunicationException;
use Predis\Protocol\ProtocolException;
use Predis\Protocol\ResponseHandlerInterface;
use Predis\ResponseError;
use Predis\Iterator\MultiBulkResponseSimple;
use Predis\ResponseQueued;
use Predis\Protocol\ProtocolInterface;

/**
 * Implements a response handler for status replies using the standard wire
 * protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseStatusHandler implements ResponseHandlerInterface
{
    /**
     * {@inheritdoc}
     */
    public function handle(ComposableConnectionInterface $connection, $status)
    {
        switch ($status) {
            case 'OK':
                return true;

            case 'QUEUED':
                return new ResponseQueued();

            default:
                return $status;
        }
    }
}

/**
 * Implements a pluggable command serializer using the standard  wire protocol
 * defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TextCommandSerializer implements CommandSerializerInterface
{
    /**
     * {@inheritdoc}
     */
    public function serialize(CommandInterface $command)
    {
        $commandId = $command->getId();
        $arguments = $command->getArguments();

        $cmdlen = strlen($commandId);
        $reqlen = count($arguments) + 1;

        $buffer = "*{$reqlen}\r\n\${$cmdlen}\r\n{$commandId}\r\n";

        for ($i = 0; $i < $reqlen - 1; $i++) {
            $argument = $arguments[$i];
            $arglen = strlen($argument);
            $buffer .= "\${$arglen}\r\n{$argument}\r\n";
        }

        return $buffer;
    }
}

/**
 * Implements a protocol processor for the standard wire protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TextProtocol implements ProtocolInterface
{
    const NEWLINE = "\r\n";
    const OK      = 'OK';
    const ERROR   = 'ERR';
    const QUEUED  = 'QUEUED';
    const NULL    = 'nil';

    const PREFIX_STATUS     = '+';
    const PREFIX_ERROR      = '-';
    const PREFIX_INTEGER    = ':';
    const PREFIX_BULK       = '$';
    const PREFIX_MULTI_BULK = '*';

    const BUFFER_SIZE = 4096;

    private $mbiterable;
    private $serializer;

    /**
     *
     */
    public function __construct()
    {
        $this->mbiterable = false;
        $this->serializer = new TextCommandSerializer();
    }

    /**
     * {@inheritdoc}
     */
    public function write(ComposableConnectionInterface $connection, CommandInterface $command)
    {
        $connection->writeBytes($this->serializer->serialize($command));
    }

    /**
     * {@inheritdoc}
     */
    public function read(ComposableConnectionInterface $connection)
    {
        $chunk = $connection->readLine();
        $prefix = $chunk[0];
        $payload = substr($chunk, 1);

        switch ($prefix) {
            case '+':    // inline
                switch ($payload) {
                    case 'OK':
                        return true;

                    case 'QUEUED':
                        return new ResponseQueued();

                    default:
                        return $payload;
                }

            case '$':    // bulk
                $size = (int) $payload;
                if ($size === -1) {
                    return null;
                }
                return substr($connection->readBytes($size + 2), 0, -2);

            case '*':    // multi bulk
                $count = (int) $payload;

                if ($count === -1) {
                    return null;
                }
                if ($this->mbiterable) {
                    return new MultiBulkResponseSimple($connection, $count);
                }

                $multibulk = array();

                for ($i = 0; $i < $count; $i++) {
                    $multibulk[$i] = $this->read($connection);
                }

                return $multibulk;

            case ':':    // integer
                return (int) $payload;

            case '-':    // error
                return new ResponseError($payload);

            default:
                CommunicationException::handle(new ProtocolException(
                    $connection, "Unknown prefix: '$prefix'"
                ));
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setOption($option, $value)
    {
        switch ($option) {
            case 'iterable_multibulk':
                $this->mbiterable = (bool) $value;
                break;
        }
    }
}

/**
 * Implements a pluggable response reader using the standard wire protocol
 * defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class TextResponseReader implements ResponseReaderInterface
{
    private $handlers;

    /**
     *
     */
    public function __construct()
    {
        $this->handlers = $this->getDefaultHandlers();
    }

    /**
     * Returns the default set of response handlers for all the type of replies
     * that can be returned by Redis.
     */
    private function getDefaultHandlers()
    {
        return array(
            TextProtocol::PREFIX_STATUS     => new ResponseStatusHandler(),
            TextProtocol::PREFIX_ERROR      => new ResponseErrorHandler(),
            TextProtocol::PREFIX_INTEGER    => new ResponseIntegerHandler(),
            TextProtocol::PREFIX_BULK       => new ResponseBulkHandler(),
            TextProtocol::PREFIX_MULTI_BULK => new ResponseMultiBulkHandler(),
        );
    }

    /**
     * Sets a response handler for a certain prefix that identifies a type of
     * reply that can be returned by Redis.
     *
     * @param string $prefix Identifier for a type of reply.
     * @param ResponseHandlerInterface $handler Response handler for the reply.
     */
    public function setHandler($prefix, ResponseHandlerInterface $handler)
    {
        $this->handlers[$prefix] = $handler;
    }

    /**
     * Returns the response handler associated to a certain type of reply that
     * can be returned by Redis.
     *
     * @param string $prefix Identifier for a type of reply.
     * @return ResponseHandlerInterface
     */
    public function getHandler($prefix)
    {
        if (isset($this->handlers[$prefix])) {
            return $this->handlers[$prefix];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function read(ComposableConnectionInterface $connection)
    {
        $header = $connection->readLine();

        if ($header === '') {
            $this->protocolError($connection, 'Unexpected empty header');
        }

        $prefix = $header[0];

        if (!isset($this->handlers[$prefix])) {
            $this->protocolError($connection, "Unknown prefix: '$prefix'");
        }

        $handler = $this->handlers[$prefix];

        return $handler->handle($connection, substr($header, 1));
    }

    /**
     * Helper method used to handle a protocol error generated while reading a
     * reply from a connection to Redis.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis that generated the error.
     * @param string $message Error message.
     */
    private function protocolError(ComposableConnectionInterface $connection, $message)
    {
        CommunicationException::handle(new ProtocolException($connection, $message));
    }
}

/**
 * Implements a response handler for iterable multi-bulk replies using the
 * standard wire protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseMultiBulkStreamHandler implements ResponseHandlerInterface
{
    /**
     * Handles a multi-bulk reply returned by Redis in a streamable fashion.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param string $lengthString Number of items in the multi-bulk reply.
     * @return MultiBulkResponseSimple
     */
    public function handle(ComposableConnectionInterface $connection, $lengthString)
    {
        $length = (int) $lengthString;

        if ("$length" != $lengthString) {
            CommunicationException::handle(new ProtocolException(
                $connection, "Cannot parse '$lengthString' as multi-bulk length"
            ));
        }

        return new MultiBulkResponseSimple($connection, $length);
    }
}

/**
 * Implements a response handler for multi-bulk replies using the standard
 * wire protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseMultiBulkHandler implements ResponseHandlerInterface
{
    /**
     * Handles a multi-bulk reply returned by Redis.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param string $lengthString Number of items in the multi-bulk reply.
     * @return array
     */
    public function handle(ComposableConnectionInterface $connection, $lengthString)
    {
        $length = (int) $lengthString;

        if ("$length" !== $lengthString) {
            CommunicationException::handle(new ProtocolException(
                $connection, "Cannot parse '$lengthString' as multi-bulk length"
            ));
        }

        if ($length === -1) {
            return null;
        }

        $list = array();

        if ($length > 0) {
            $handlersCache = array();
            $reader = $connection->getProtocol()->getReader();

            for ($i = 0; $i < $length; $i++) {
                $header = $connection->readLine();
                $prefix = $header[0];

                if (isset($handlersCache[$prefix])) {
                    $handler = $handlersCache[$prefix];
                } else {
                    $handler = $reader->getHandler($prefix);
                    $handlersCache[$prefix] = $handler;
                }

                $list[$i] = $handler->handle($connection, substr($header, 1));
            }
        }

        return $list;
    }
}

/**
 * Implements a response handler for bulk replies using the standard wire
 * protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseBulkHandler implements ResponseHandlerInterface
{
    /**
     * Handles a bulk reply returned by Redis.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param string $lengthString Bytes size of the bulk reply.
     * @return string
     */
    public function handle(ComposableConnectionInterface $connection, $lengthString)
    {
        $length = (int) $lengthString;

        if ("$length" !== $lengthString) {
            CommunicationException::handle(new ProtocolException(
                $connection, "Cannot parse '$lengthString' as bulk length"
            ));
        }

        if ($length >= 0) {
            return substr($connection->readBytes($length + 2), 0, -2);
        }

        if ($length == -1) {
            return null;
        }
    }
}

/**
 * Implements a response handler for error replies using the standard wire
 * protocol defined by Redis.
 *
 * This handler returns a reply object to notify the user that an error has
 * occurred on the server.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseErrorHandler implements ResponseHandlerInterface
{
    /**
     * {@inheritdoc}
     */
    public function handle(ComposableConnectionInterface $connection, $errorMessage)
    {
        return new ResponseError($errorMessage);
    }
}

/**
 * Implements a response handler for integer replies using the standard wire
 * protocol defined by Redis.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ResponseIntegerHandler implements ResponseHandlerInterface
{
    /**
     * Handles an integer reply returned by Redis.
     *
     * @param ComposableConnectionInterface $connection Connection to Redis.
     * @param string $number String representation of an integer.
     * @return int
     */
    public function handle(ComposableConnectionInterface $connection, $number)
    {
        if (is_numeric($number)) {
            return (int) $number;
        }

        if ($number !== 'nil') {
            CommunicationException::handle(new ProtocolException(
                $connection, "Cannot parse '$number' as numeric response"
            ));
        }

        return null;
    }
}

/**
 * Implements a customizable protocol processor that uses the standard Redis
 * wire protocol to serialize Redis commands and parse replies returned by
 * the server using a pluggable set of classes.
 *
 * @link http://redis.io/topics/protocol
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ComposableTextProtocol implements ComposableProtocolInterface
{
    private $serializer;
    private $reader;

    /**
     * @param array $options Set of options used to initialize the protocol processor.
     */
    public function __construct(Array $options = array())
    {
        $this->setSerializer(new TextCommandSerializer());
        $this->setReader(new TextResponseReader());

        if (count($options) > 0) {
            $this->initializeOptions($options);
        }
    }

    /**
     * Initializes the protocol processor using a set of options.
     *
     * @param array $options Set of options.
     */
    private function initializeOptions(Array $options)
    {
        foreach ($options as $k => $v) {
            $this->setOption($k, $v);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setOption($option, $value)
    {
        switch ($option) {
            case 'iterable_multibulk':
                $handler = $value ? new ResponseMultiBulkStreamHandler() : new ResponseMultiBulkHandler();
                $this->reader->setHandler(TextProtocol::PREFIX_MULTI_BULK, $handler);
                break;

            default:
                throw new \InvalidArgumentException("The option $option is not supported by the current protocol");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function serialize(CommandInterface $command)
    {
        return $this->serializer->serialize($command);
    }

    /**
     * {@inheritdoc}
     */
    public function write(ComposableConnectionInterface $connection, CommandInterface $command)
    {
        $connection->writeBytes($this->serializer->serialize($command));
    }

    /**
     * {@inheritdoc}
     */
    public function read(ComposableConnectionInterface $connection)
    {
        return $this->reader->read($connection);
    }

    /**
     * {@inheritdoc}
     */
    public function setSerializer(CommandSerializerInterface $serializer)
    {
        $this->serializer = $serializer;
    }

    /**
     * {@inheritdoc}
     */
    public function getSerializer()
    {
        return $this->serializer;
    }

    /**
     * {@inheritdoc}
     */
    public function setReader(ResponseReaderInterface $reader)
    {
        $this->reader = $reader;
    }

    /**
     * {@inheritdoc}
     */
    public function getReader()
    {
        return $this->reader;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Cluster\Distribution;

use Predis\Cluster\Hash\HashGeneratorInterface;

/**
 * A distributor implements the logic to automatically distribute
 * keys among several nodes for client-side sharding.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface DistributionStrategyInterface
{
    /**
     * Adds a node to the distributor with an optional weight.
     *
     * @param mixed $node Node object.
     * @param int $weight Weight for the node.
     */
    public function add($node, $weight = null);

    /**
     * Removes a node from the distributor.
     *
     * @param mixed $node Node object.
     */
    public function remove($node);

    /**
     * Gets a node from the distributor using the computed hash of a key.
     *
     * @return mixed
     */
    public function get($key);

    /**
     * Returns the underlying hash generator instance.
     *
     * @return HashGeneratorInterface
     */
    public function getHashGenerator();
}

/**
 * This class implements an hashring-based distributor that uses the same
 * algorithm of memcache to distribute keys in a cluster using client-side
 * sharding.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 * @author Lorenzo Castelli <lcastelli@gmail.com>
 */
class HashRing implements DistributionStrategyInterface, HashGeneratorInterface
{
    const DEFAULT_REPLICAS = 128;
    const DEFAULT_WEIGHT   = 100;

    private $ring;
    private $ringKeys;
    private $ringKeysCount;
    private $replicas;
    private $nodeHashCallback;
    private $nodes = array();

    /**
     * @param int $replicas Number of replicas in the ring.
     * @param mixed $nodeHashCallback Callback returning the string used to calculate the hash of a node.
     */
    public function __construct($replicas = self::DEFAULT_REPLICAS, $nodeHashCallback = null)
    {
        $this->replicas = $replicas;
        $this->nodeHashCallback = $nodeHashCallback;
    }

    /**
     * Adds a node to the ring with an optional weight.
     *
     * @param mixed $node Node object.
     * @param int $weight Weight for the node.
     */
    public function add($node, $weight = null)
    {
        // In case of collisions in the hashes of the nodes, the node added
        // last wins, thus the order in which nodes are added is significant.
        $this->nodes[] = array('object' => $node, 'weight' => (int) $weight ?: $this::DEFAULT_WEIGHT);
        $this->reset();
    }

    /**
     * {@inheritdoc}
     */
    public function remove($node)
    {
        // A node is removed by resetting the ring so that it's recreated from
        // scratch, in order to reassign possible hashes with collisions to the
        // right node according to the order in which they were added in the
        // first place.
        for ($i = 0; $i < count($this->nodes); ++$i) {
            if ($this->nodes[$i]['object'] === $node) {
                array_splice($this->nodes, $i, 1);
                $this->reset();
                break;
            }
        }
    }

    /**
     * Resets the distributor.
     */
    private function reset()
    {
        unset(
            $this->ring,
            $this->ringKeys,
            $this->ringKeysCount
        );
    }

    /**
     * Returns the initialization status of the distributor.
     *
     * @return Boolean
     */
    private function isInitialized()
    {
        return isset($this->ringKeys);
    }

    /**
     * Calculates the total weight of all the nodes in the distributor.
     *
     * @return int
     */
    private function computeTotalWeight()
    {
        $totalWeight = 0;

        foreach ($this->nodes as $node) {
            $totalWeight += $node['weight'];
        }

        return $totalWeight;
    }

    /**
     * Initializes the distributor.
     */
    private function initialize()
    {
        if ($this->isInitialized()) {
            return;
        }

        if (!$this->nodes) {
            throw new EmptyRingException('Cannot initialize empty hashring');
        }

        $this->ring = array();
        $totalWeight = $this->computeTotalWeight();
        $nodesCount = count($this->nodes);

        foreach ($this->nodes as $node) {
            $weightRatio = $node['weight'] / $totalWeight;
            $this->addNodeToRing($this->ring, $node, $nodesCount, $this->replicas, $weightRatio);
        }

        ksort($this->ring, SORT_NUMERIC);
        $this->ringKeys = array_keys($this->ring);
        $this->ringKeysCount = count($this->ringKeys);
    }

    /**
     * Implements the logic needed to add a node to the hashring.
     *
     * @param array $ring Source hashring.
     * @param mixed $node Node object to be added.
     * @param int $totalNodes Total number of nodes.
     * @param int $replicas Number of replicas in the ring.
     * @param float $weightRatio Weight ratio for the node.
     */
    protected function addNodeToRing(&$ring, $node, $totalNodes, $replicas, $weightRatio)
    {
        $nodeObject = $node['object'];
        $nodeHash = $this->getNodeHash($nodeObject);
        $replicas = (int) round($weightRatio * $totalNodes * $replicas);

        for ($i = 0; $i < $replicas; $i++) {
            $key = crc32("$nodeHash:$i");
            $ring[$key] = $nodeObject;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function getNodeHash($nodeObject)
    {
        if ($this->nodeHashCallback === null) {
            return (string) $nodeObject;
        }

        return call_user_func($this->nodeHashCallback, $nodeObject);
    }

    /**
     * Calculates the hash for the specified value.
     *
     * @param string $value Input value.
     * @return int
     */
    public function hash($value)
    {
        return crc32($value);
    }

    /**
     * {@inheritdoc}
     */
    public function get($key)
    {
        return $this->ring[$this->getNodeKey($key)];
    }

    /**
     * Calculates the corrisponding key of a node distributed in the hashring.
     *
     * @param int $key Computed hash of a key.
     * @return int
     */
    private function getNodeKey($key)
    {
        $this->initialize();
        $ringKeys = $this->ringKeys;
        $upper = $this->ringKeysCount - 1;
        $lower = 0;

        while ($lower <= $upper) {
            $index = ($lower + $upper) >> 1;
            $item  = $ringKeys[$index];

            if ($item > $key) {
                $upper = $index - 1;
            } else if ($item < $key) {
                $lower = $index + 1;
            } else {
                return $item;
            }
        }

        return $ringKeys[$this->wrapAroundStrategy($upper, $lower, $this->ringKeysCount)];
    }

    /**
     * Implements a strategy to deal with wrap-around errors during binary searches.
     *
     * @param int $upper
     * @param int $lower
     * @param int $ringKeysCount
     * @return int
     */
    protected function wrapAroundStrategy($upper, $lower, $ringKeysCount)
    {
        // Binary search for the last item in ringkeys with a value less or
        // equal to the key. If no such item exists, return the last item.
        return $upper >= 0 ? $upper : $ringKeysCount - 1;
    }

    /**
     * {@inheritdoc}
     */
    public function getHashGenerator()
    {
        return $this;
    }
}

/**
 * This class implements an hashring-based distributor that uses the same
 * algorithm of libketama to distribute keys in a cluster using client-side
 * sharding.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 * @author Lorenzo Castelli <lcastelli@gmail.com>
 */
class KetamaPureRing extends HashRing
{
    const DEFAULT_REPLICAS = 160;

    /**
     * @param mixed $nodeHashCallback Callback returning the string used to calculate the hash of a node.
     */
    public function __construct($nodeHashCallback = null)
    {
        parent::__construct($this::DEFAULT_REPLICAS, $nodeHashCallback);
    }

    /**
     * {@inheritdoc}
     */
    protected function addNodeToRing(&$ring, $node, $totalNodes, $replicas, $weightRatio)
    {
        $nodeObject = $node['object'];
        $nodeHash = $this->getNodeHash($nodeObject);
        $replicas = (int) floor($weightRatio * $totalNodes * ($replicas / 4));

        for ($i = 0; $i < $replicas; $i++) {
            $unpackedDigest = unpack('V4', md5("$nodeHash-$i", true));

            foreach ($unpackedDigest as $key) {
                $ring[$key] = $nodeObject;
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function hash($value)
    {
        $hash = unpack('V', md5($value, true));
        return $hash[1];
    }

    /**
     * {@inheritdoc}
     */
    protected function wrapAroundStrategy($upper, $lower, $ringKeysCount)
    {
        // Binary search for the first item in _ringkeys with a value greater
        // or equal to the key. If no such item exists, return the first item.
        return $lower < $ringKeysCount ? $lower : 0;
    }
}

/**
 * Exception class that identifies empty rings.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class EmptyRingException extends \Exception
{
}

/* --------------------------------------------------------------------------- */

namespace Predis\Iterator;

use Predis\ResponseObjectInterface;
use Predis\Connection\SingleConnectionInterface;

/**
 * Iterator that abstracts the access to multibulk replies and allows
 * them to be consumed by user's code in a streaming fashion.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class MultiBulkResponse implements \Iterator, \Countable, ResponseObjectInterface
{
    protected $position;
    protected $current;
    protected $replySize;

    /**
     * {@inheritdoc}
     */
    public function rewind()
    {
        // NOOP
    }

    /**
     * {@inheritdoc}
     */
    public function current()
    {
        return $this->current;
    }

    /**
     * {@inheritdoc}
     */
    public function key()
    {
        return $this->position;
    }

    /**
     * {@inheritdoc}
     */
    public function next()
    {
        if (++$this->position < $this->replySize) {
            $this->current = $this->getValue();
        }

        return $this->position;
    }

    /**
     * {@inheritdoc}
     */
    public function valid()
    {
        return $this->position < $this->replySize;
    }

    /**
     * Returns the number of items of the whole multibulk reply.
     *
     * This method should be used to get the size of the current multibulk
     * reply without using iterator_count, which actually consumes the
     * iterator to calculate the size (rewinding is not supported).
     *
     * @return int
     */
    public function count()
    {
        return $this->replySize;
    }

    /**
     * Returns the current position of the iterator.
     *
     * @return int
     */
    public function getPosition()
    {
        return $this->position;
    }

    /**
     * {@inheritdoc}
     */
    protected abstract function getValue();
}

/**
 * Abstracts the access to a streamable list of tuples represented
 * as a multibulk reply that alternates keys and values.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MultiBulkResponseTuple extends MultiBulkResponse implements \OuterIterator
{
    private $iterator;

    /**
     * @param MultiBulkResponseSimple $iterator Multibulk reply iterator.
     */
    public function __construct(MultiBulkResponseSimple $iterator)
    {
        $this->checkPreconditions($iterator);

        $virtualSize = count($iterator) / 2;
        $this->iterator = $iterator;
        $this->position = $iterator->getPosition();
        $this->current = $virtualSize > 0 ? $this->getValue() : null;
        $this->replySize = $virtualSize;
    }

    /**
     * Checks for valid preconditions.
     *
     * @param MultiBulkResponseSimple $iterator Multibulk reply iterator.
     */
    protected function checkPreconditions(MultiBulkResponseSimple $iterator)
    {
        if ($iterator->getPosition() !== 0) {
            throw new \RuntimeException('Cannot initialize a tuple iterator with an already initiated iterator');
        }

        if (($size = count($iterator)) % 2 !== 0) {
            throw new \UnexpectedValueException("Invalid reply size for a tuple iterator [$size]");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getInnerIterator()
    {
        return $this->iterator;
    }

    /**
     * {@inheritdoc}
     */
    public function __destruct()
    {
        $this->iterator->sync(true);
    }

    /**
     * {@inheritdoc}
     */
    protected function getValue()
    {
        $k = $this->iterator->current();
        $this->iterator->next();

        $v = $this->iterator->current();
        $this->iterator->next();

        return array($k, $v);
    }
}

/**
 * Streams a multibulk reply.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MultiBulkResponseSimple extends MultiBulkResponse
{
    private $connection;

    /**
     * @param SingleConnectionInterface $connection Connection to Redis.
     * @param int $size Number of elements of the multibulk reply.
     */
    public function __construct(SingleConnectionInterface $connection, $size)
    {
        $this->connection = $connection;
        $this->position = 0;
        $this->current = $size > 0 ? $this->getValue() : null;
        $this->replySize = $size;
    }

    /**
     * Handles the synchronization of the client with the Redis protocol
     * then PHP's garbage collector kicks in (e.g. then the iterator goes
     * out of the scope of a foreach).
     */
    public function __destruct()
    {
        $this->sync(true);
    }

    /**
     * Synchronizes the client with the queued elements that have not been
     * read from the connection by consuming the rest of the multibulk reply,
     * or simply by dropping the connection.
     *
     * @param Boolean $drop True to synchronize the client by dropping the connection.
     *                      False to synchronize the client by consuming the multibulk reply.
     */
    public function sync($drop = false)
    {
        if ($drop == true) {
            if ($this->valid()) {
                $this->position = $this->replySize;
                $this->connection->disconnect();
            }
        } else {
            while ($this->valid()) {
                $this->next();
            }
        }
    }

    /**
     * Reads the next item of the multibulk reply from the server.
     *
     * @return mixed
     */
    protected function getValue()
    {
        return $this->connection->read();
    }

    /**
     * Returns an iterator that reads the multi-bulk response as
     * list of tuples.
     *
     * @return MultiBulkResponseTuple
     */
    public function asTuple()
    {
        return new MultiBulkResponseTuple($this);
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Cluster\Hash;

/**
 * A generator of node keys implements the logic used to calculate the hash of
 * a key to distribute the respective operations among nodes.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface HashGeneratorInterface
{
    /**
     * Generates an hash that is used by the distributor algorithm
     *
     * @param string $value Value used to generate the hash.
     * @return int
     */
    public function hash($value);
}

/**
 * This class implements the CRC-CCITT-16 algorithm used by redis-cluster.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class CRC16HashGenerator implements HashGeneratorInterface
{
    private static $CCITT_16 = array(
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
        0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
        0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
        0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
        0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
        0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
        0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
        0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
        0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
        0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
        0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
        0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
        0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
        0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
        0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
        0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
        0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
        0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
        0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
        0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
        0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
        0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
        0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0,
    );

    /**
     * {@inheritdoc}
     */
    public function hash($value)
    {
        // CRC-CCITT-16 algorithm
        $crc = 0;
        $CCITT_16 = self::$CCITT_16;
        $strlen = strlen($value);

        for ($i = 0; $i < $strlen; $i++) {
            $crc = (($crc << 8) ^ $CCITT_16[($crc >> 8) ^ ord($value[$i])]) & 0xFFFF;
        }

        return $crc;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Cluster;

use Predis\Command\CommandInterface;
use Predis\Cluster\Hash\HashGeneratorInterface;
use Predis\Command\ScriptedCommand;
use Predis\Cluster\Hash\CRC16HashGenerator;

/**
 * Interface for classes defining the strategy used to calculate an hash
 * out of keys extracted from supported commands.
 *
 * This is mostly useful to support clustering via client-side sharding.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
interface CommandHashStrategyInterface
{
    /**
     * Returns the hash for the given command using the specified algorithm, or null
     * if the command cannot be hashed.
     *
     * @param CommandInterface $command Command to be hashed.
     * @return int
     */
    public function getHash(CommandInterface $command);

    /**
     * Returns the hash for the given key using the specified algorithm.
     *
     * @param string $key Key to be hashed.
     * @return string
     */
    public function getKeyHash($key);
}

/**
 * Default class used by Predis to calculate hashes out of keys of
 * commands supported by redis-cluster.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class RedisClusterHashStrategy implements CommandHashStrategyInterface
{
    private $commands;
    private $hashGenerator;

    /**
     *
     */
    public function __construct()
    {
        $this->commands = $this->getDefaultCommands();
        $this->hashGenerator = new CRC16HashGenerator();
    }

    /**
     * Returns the default map of supported commands with their handlers.
     *
     * @return array
     */
    protected function getDefaultCommands()
    {
        $keyIsFirstArgument = array($this, 'getKeyFromFirstArgument');

        return array(
            /* commands operating on the key space */
            'EXISTS'                => $keyIsFirstArgument,
            'DEL'                   => array($this, 'getKeyFromAllArguments'),
            'TYPE'                  => $keyIsFirstArgument,
            'EXPIRE'                => $keyIsFirstArgument,
            'EXPIREAT'              => $keyIsFirstArgument,
            'PERSIST'               => $keyIsFirstArgument,
            'PEXPIRE'               => $keyIsFirstArgument,
            'PEXPIREAT'             => $keyIsFirstArgument,
            'TTL'                   => $keyIsFirstArgument,
            'PTTL'                  => $keyIsFirstArgument,
            'SORT'                  => $keyIsFirstArgument, // TODO

            /* commands operating on string values */
            'APPEND'                => $keyIsFirstArgument,
            'DECR'                  => $keyIsFirstArgument,
            'DECRBY'                => $keyIsFirstArgument,
            'GET'                   => $keyIsFirstArgument,
            'GETBIT'                => $keyIsFirstArgument,
            'MGET'                  => array($this, 'getKeyFromAllArguments'),
            'SET'                   => $keyIsFirstArgument,
            'GETRANGE'              => $keyIsFirstArgument,
            'GETSET'                => $keyIsFirstArgument,
            'INCR'                  => $keyIsFirstArgument,
            'INCRBY'                => $keyIsFirstArgument,
            'SETBIT'                => $keyIsFirstArgument,
            'SETEX'                 => $keyIsFirstArgument,
            'MSET'                  => array($this, 'getKeyFromInterleavedArguments'),
            'MSETNX'                => array($this, 'getKeyFromInterleavedArguments'),
            'SETNX'                 => $keyIsFirstArgument,
            'SETRANGE'              => $keyIsFirstArgument,
            'STRLEN'                => $keyIsFirstArgument,
            'SUBSTR'                => $keyIsFirstArgument,
            'BITCOUNT'              => $keyIsFirstArgument,

            /* commands operating on lists */
            'LINSERT'               => $keyIsFirstArgument,
            'LINDEX'                => $keyIsFirstArgument,
            'LLEN'                  => $keyIsFirstArgument,
            'LPOP'                  => $keyIsFirstArgument,
            'RPOP'                  => $keyIsFirstArgument,
            'BLPOP'                 => array($this, 'getKeyFromBlockingListCommands'),
            'BRPOP'                 => array($this, 'getKeyFromBlockingListCommands'),
            'LPUSH'                 => $keyIsFirstArgument,
            'LPUSHX'                => $keyIsFirstArgument,
            'RPUSH'                 => $keyIsFirstArgument,
            'RPUSHX'                => $keyIsFirstArgument,
            'LRANGE'                => $keyIsFirstArgument,
            'LREM'                  => $keyIsFirstArgument,
            'LSET'                  => $keyIsFirstArgument,
            'LTRIM'                 => $keyIsFirstArgument,

            /* commands operating on sets */
            'SADD'                  => $keyIsFirstArgument,
            'SCARD'                 => $keyIsFirstArgument,
            'SISMEMBER'             => $keyIsFirstArgument,
            'SMEMBERS'              => $keyIsFirstArgument,
            'SPOP'                  => $keyIsFirstArgument,
            'SRANDMEMBER'           => $keyIsFirstArgument,
            'SREM'                  => $keyIsFirstArgument,

            /* commands operating on sorted sets */
            'ZADD'                  => $keyIsFirstArgument,
            'ZCARD'                 => $keyIsFirstArgument,
            'ZCOUNT'                => $keyIsFirstArgument,
            'ZINCRBY'               => $keyIsFirstArgument,
            'ZRANGE'                => $keyIsFirstArgument,
            'ZRANGEBYSCORE'         => $keyIsFirstArgument,
            'ZRANK'                 => $keyIsFirstArgument,
            'ZREM'                  => $keyIsFirstArgument,
            'ZREMRANGEBYRANK'       => $keyIsFirstArgument,
            'ZREMRANGEBYSCORE'      => $keyIsFirstArgument,
            'ZREVRANGE'             => $keyIsFirstArgument,
            'ZREVRANGEBYSCORE'      => $keyIsFirstArgument,
            'ZREVRANK'              => $keyIsFirstArgument,
            'ZSCORE'                => $keyIsFirstArgument,

            /* commands operating on hashes */
            'HDEL'                  => $keyIsFirstArgument,
            'HEXISTS'               => $keyIsFirstArgument,
            'HGET'                  => $keyIsFirstArgument,
            'HGETALL'               => $keyIsFirstArgument,
            'HMGET'                 => $keyIsFirstArgument,
            'HMSET'                 => $keyIsFirstArgument,
            'HINCRBY'               => $keyIsFirstArgument,
            'HINCRBYFLOAT'          => $keyIsFirstArgument,
            'HKEYS'                 => $keyIsFirstArgument,
            'HLEN'                  => $keyIsFirstArgument,
            'HSET'                  => $keyIsFirstArgument,
            'HSETNX'                => $keyIsFirstArgument,
            'HVALS'                 => $keyIsFirstArgument,

            /* scripting */
            'EVAL'                  => array($this, 'getKeyFromScriptingCommands'),
            'EVALSHA'               => array($this, 'getKeyFromScriptingCommands'),
        );
    }

    /**
     * Returns the list of IDs for the supported commands.
     *
     * @return array
     */
    public function getSupportedCommands()
    {
        return array_keys($this->commands);
    }

    /**
     * Sets an handler for the specified command ID.
     *
     * The signature of the callback must have a single parameter
     * of type Predis\Command\CommandInterface.
     *
     * When the callback argument is omitted or NULL, the previously
     * associated handler for the specified command ID is removed.
     *
     * @param string $commandId The ID of the command to be handled.
     * @param mixed $callback A valid callable object or NULL.
     */
    public function setCommandHandler($commandId, $callback = null)
    {
        $commandId = strtoupper($commandId);

        if (!isset($callback)) {
            unset($this->commands[$commandId]);
            return;
        }

        if (!is_callable($callback)) {
            throw new \InvalidArgumentException("Callback must be a valid callable object or NULL");
        }

        $this->commands[$commandId] = $callback;
    }

    /**
     * Extracts the key from the first argument of a command instance.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromFirstArgument(CommandInterface $command)
    {
        return $command->getArgument(0);
    }

    /**
     * Extracts the key from a command that can accept multiple keys ensuring
     * that only one key is actually specified to comply with redis-cluster.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromAllArguments(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if (count($arguments) === 1) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from a command that can accept multiple keys ensuring
     * that only one key is actually specified to comply with redis-cluster.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromInterleavedArguments(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if (count($arguments) === 2) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from BLPOP and BRPOP commands ensuring that only one key
     * is actually specified to comply with redis-cluster.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromBlockingListCommands(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if (count($arguments) === 2) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from EVAL and EVALSHA commands.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromScriptingCommands(CommandInterface $command)
    {
        if ($command instanceof ScriptedCommand) {
            $keys = $command->getKeys();
        } else {
            $keys = array_slice($args = $command->getArguments(), 2, $args[1]);
        }

        if (count($keys) === 1) {
            return $keys[0];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getHash(CommandInterface $command)
    {
        $hash = $command->getHash();

        if (!isset($hash) && isset($this->commands[$cmdID = $command->getId()])) {
            $key = call_user_func($this->commands[$cmdID], $command);

            if (isset($key)) {
                $hash = $this->hashGenerator->hash($key);
                $command->setHash($hash);
            }
        }

        return $hash;
    }

    /**
     * {@inheritdoc}
     */
    public function getKeyHash($key)
    {
        return $this->hashGenerator->hash($key);
    }
}

/**
 * Default class used by Predis for client-side sharding to calculate
 * hashes out of keys of supported commands.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PredisClusterHashStrategy implements CommandHashStrategyInterface
{
    private $commands;
    private $hashGenerator;

    /**
     * @param HashGeneratorInterface $hashGenerator Hash generator instance.
     */
    public function __construct(HashGeneratorInterface $hashGenerator)
    {
        $this->commands = $this->getDefaultCommands();
        $this->hashGenerator = $hashGenerator;
    }

    /**
     * Returns the default map of supported commands with their handlers.
     *
     * @return array
     */
    protected function getDefaultCommands()
    {
        $keyIsFirstArgument = array($this, 'getKeyFromFirstArgument');
        $keysAreAllArguments = array($this, 'getKeyFromAllArguments');

        return array(
            /* commands operating on the key space */
            'EXISTS'                => $keyIsFirstArgument,
            'DEL'                   => $keysAreAllArguments,
            'TYPE'                  => $keyIsFirstArgument,
            'EXPIRE'                => $keyIsFirstArgument,
            'EXPIREAT'              => $keyIsFirstArgument,
            'PERSIST'               => $keyIsFirstArgument,
            'PEXPIRE'               => $keyIsFirstArgument,
            'PEXPIREAT'             => $keyIsFirstArgument,
            'TTL'                   => $keyIsFirstArgument,
            'PTTL'                  => $keyIsFirstArgument,
            'SORT'                  => $keyIsFirstArgument, // TODO
            'DUMP'                  => $keyIsFirstArgument,
            'RESTORE'               => $keyIsFirstArgument,

            /* commands operating on string values */
            'APPEND'                => $keyIsFirstArgument,
            'DECR'                  => $keyIsFirstArgument,
            'DECRBY'                => $keyIsFirstArgument,
            'GET'                   => $keyIsFirstArgument,
            'GETBIT'                => $keyIsFirstArgument,
            'MGET'                  => $keysAreAllArguments,
            'SET'                   => $keyIsFirstArgument,
            'GETRANGE'              => $keyIsFirstArgument,
            'GETSET'                => $keyIsFirstArgument,
            'INCR'                  => $keyIsFirstArgument,
            'INCRBY'                => $keyIsFirstArgument,
            'SETBIT'                => $keyIsFirstArgument,
            'SETEX'                 => $keyIsFirstArgument,
            'MSET'                  => array($this, 'getKeyFromInterleavedArguments'),
            'MSETNX'                => array($this, 'getKeyFromInterleavedArguments'),
            'SETNX'                 => $keyIsFirstArgument,
            'SETRANGE'              => $keyIsFirstArgument,
            'STRLEN'                => $keyIsFirstArgument,
            'SUBSTR'                => $keyIsFirstArgument,
            'BITOP'                 => array($this, 'getKeyFromBitOp'),
            'BITCOUNT'              => $keyIsFirstArgument,

            /* commands operating on lists */
            'LINSERT'               => $keyIsFirstArgument,
            'LINDEX'                => $keyIsFirstArgument,
            'LLEN'                  => $keyIsFirstArgument,
            'LPOP'                  => $keyIsFirstArgument,
            'RPOP'                  => $keyIsFirstArgument,
            'RPOPLPUSH'             => $keysAreAllArguments,
            'BLPOP'                 => array($this, 'getKeyFromBlockingListCommands'),
            'BRPOP'                 => array($this, 'getKeyFromBlockingListCommands'),
            'BRPOPLPUSH'            => array($this, 'getKeyFromBlockingListCommands'),
            'LPUSH'                 => $keyIsFirstArgument,
            'LPUSHX'                => $keyIsFirstArgument,
            'RPUSH'                 => $keyIsFirstArgument,
            'RPUSHX'                => $keyIsFirstArgument,
            'LRANGE'                => $keyIsFirstArgument,
            'LREM'                  => $keyIsFirstArgument,
            'LSET'                  => $keyIsFirstArgument,
            'LTRIM'                 => $keyIsFirstArgument,

            /* commands operating on sets */
            'SADD'                  => $keyIsFirstArgument,
            'SCARD'                 => $keyIsFirstArgument,
            'SDIFF'                 => $keysAreAllArguments,
            'SDIFFSTORE'            => $keysAreAllArguments,
            'SINTER'                => $keysAreAllArguments,
            'SINTERSTORE'           => $keysAreAllArguments,
            'SUNION'                => $keysAreAllArguments,
            'SUNIONSTORE'           => $keysAreAllArguments,
            'SISMEMBER'             => $keyIsFirstArgument,
            'SMEMBERS'              => $keyIsFirstArgument,
            'SPOP'                  => $keyIsFirstArgument,
            'SRANDMEMBER'           => $keyIsFirstArgument,
            'SREM'                  => $keyIsFirstArgument,

            /* commands operating on sorted sets */
            'ZADD'                  => $keyIsFirstArgument,
            'ZCARD'                 => $keyIsFirstArgument,
            'ZCOUNT'                => $keyIsFirstArgument,
            'ZINCRBY'               => $keyIsFirstArgument,
            'ZINTERSTORE'           => array($this, 'getKeyFromZsetAggregationCommands'),
            'ZRANGE'                => $keyIsFirstArgument,
            'ZRANGEBYSCORE'         => $keyIsFirstArgument,
            'ZRANK'                 => $keyIsFirstArgument,
            'ZREM'                  => $keyIsFirstArgument,
            'ZREMRANGEBYRANK'       => $keyIsFirstArgument,
            'ZREMRANGEBYSCORE'      => $keyIsFirstArgument,
            'ZREVRANGE'             => $keyIsFirstArgument,
            'ZREVRANGEBYSCORE'      => $keyIsFirstArgument,
            'ZREVRANK'              => $keyIsFirstArgument,
            'ZSCORE'                => $keyIsFirstArgument,
            'ZUNIONSTORE'           => array($this, 'getKeyFromZsetAggregationCommands'),

            /* commands operating on hashes */
            'HDEL'                  => $keyIsFirstArgument,
            'HEXISTS'               => $keyIsFirstArgument,
            'HGET'                  => $keyIsFirstArgument,
            'HGETALL'               => $keyIsFirstArgument,
            'HMGET'                 => $keyIsFirstArgument,
            'HMSET'                 => $keyIsFirstArgument,
            'HINCRBY'               => $keyIsFirstArgument,
            'HINCRBYFLOAT'          => $keyIsFirstArgument,
            'HKEYS'                 => $keyIsFirstArgument,
            'HLEN'                  => $keyIsFirstArgument,
            'HSET'                  => $keyIsFirstArgument,
            'HSETNX'                => $keyIsFirstArgument,
            'HVALS'                 => $keyIsFirstArgument,

            /* scripting */
            'EVAL'                  => array($this, 'getKeyFromScriptingCommands'),
            'EVALSHA'               => array($this, 'getKeyFromScriptingCommands'),
        );
    }

    /**
     * Returns the list of IDs for the supported commands.
     *
     * @return array
     */
    public function getSupportedCommands()
    {
        return array_keys($this->commands);
    }

    /**
     * Sets an handler for the specified command ID.
     *
     * The signature of the callback must have a single parameter
     * of type Predis\Command\CommandInterface.
     *
     * When the callback argument is omitted or NULL, the previously
     * associated handler for the specified command ID is removed.
     *
     * @param string $commandId The ID of the command to be handled.
     * @param mixed $callback A valid callable object or NULL.
     */
    public function setCommandHandler($commandId, $callback = null)
    {
        $commandId = strtoupper($commandId);

        if (!isset($callback)) {
            unset($this->commands[$commandId]);
            return;
        }

        if (!is_callable($callback)) {
            throw new \InvalidArgumentException("Callback must be a valid callable object or NULL");
        }

        $this->commands[$commandId] = $callback;
    }

    /**
     * Extracts the key from the first argument of a command instance.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromFirstArgument(CommandInterface $command)
    {
        return $command->getArgument(0);
    }

    /**
     * Extracts the key from a command with multiple keys only when all keys
     * in the arguments array produce the same hash.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromAllArguments(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if ($this->checkSameHashForKeys($arguments)) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from a command with multiple keys only when all keys
     * in the arguments array produce the same hash.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromInterleavedArguments(CommandInterface $command)
    {
        $arguments = $command->getArguments();
        $keys = array();

        for ($i = 0; $i < count($arguments); $i += 2) {
            $keys[] = $arguments[$i];
        }

        if ($this->checkSameHashForKeys($keys)) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from BLPOP and BRPOP commands.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromBlockingListCommands(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if ($this->checkSameHashForKeys(array_slice($arguments, 0, count($arguments) - 1))) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from BITOP command.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromBitOp(CommandInterface $command)
    {
        $arguments = $command->getArguments();

        if ($this->checkSameHashForKeys(array_slice($arguments, 1, count($arguments)))) {
            return $arguments[1];
        }
    }

    /**
     * Extracts the key from ZINTERSTORE and ZUNIONSTORE commands.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromZsetAggregationCommands(CommandInterface $command)
    {
        $arguments = $command->getArguments();
        $keys = array_merge(array($arguments[0]), array_slice($arguments, 2, $arguments[1]));

        if ($this->checkSameHashForKeys($keys)) {
            return $arguments[0];
        }
    }

    /**
     * Extracts the key from EVAL and EVALSHA commands.
     *
     * @param CommandInterface $command Command instance.
     * @return string
     */
    protected function getKeyFromScriptingCommands(CommandInterface $command)
    {
        if ($command instanceof ScriptedCommand) {
            $keys = $command->getKeys();
        } else {
            $keys = array_slice($args = $command->getArguments(), 2, $args[1]);
        }

        if ($keys && $this->checkSameHashForKeys($keys)) {
            return $keys[0];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getHash(CommandInterface $command)
    {
        $hash = $command->getHash();

        if (!isset($hash) && isset($this->commands[$cmdID = $command->getId()])) {
            $key = call_user_func($this->commands[$cmdID], $command);

            if (isset($key)) {
                $hash = $this->getKeyHash($key);
                $command->setHash($hash);
            }
        }

        return $hash;
    }

    /**
     * {@inheritdoc}
     */
    public function getKeyHash($key)
    {
        $key = $this->extractKeyTag($key);
        $hash = $this->hashGenerator->hash($key);

        return $hash;
    }

    /**
     * Checks if the specified array of keys will generate the same hash.
     *
     * @param array $keys Array of keys.
     * @return Boolean
     */
    protected function checkSameHashForKeys(Array $keys)
    {
        if (!$count = count($keys)) {
            return false;
        }

        $currentKey = $this->extractKeyTag($keys[0]);

        for ($i = 1; $i < $count; $i++) {
            $nextKey = $this->extractKeyTag($keys[$i]);

            if ($currentKey !== $nextKey) {
                return false;
            }

            $currentKey = $nextKey;
        }

        return true;
    }

    /**
     * Returns only the hashable part of a key (delimited by "{...}"), or the
     * whole key if a key tag is not found in the string.
     *
     * @param string $key A key.
     * @return string
     */
    protected function extractKeyTag($key)
    {
        if (false !== $start = strpos($key, '{')) {
            if (false !== $end = strpos($key, '}', $start)) {
                $key = substr($key, ++$start, $end - $start);
            }
        }

        return $key;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\PubSub;

use Predis\ClientInterface;
use Predis\ClientException;
use Predis\Command\AbstractCommand;
use Predis\NotSupportedException;
use Predis\Connection\AggregatedConnectionInterface;

/**
 * Client-side abstraction of a Publish / Subscribe context.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
abstract class AbstractPubSubContext implements \Iterator
{
    const SUBSCRIBE    = 'subscribe';
    const UNSUBSCRIBE  = 'unsubscribe';
    const PSUBSCRIBE   = 'psubscribe';
    const PUNSUBSCRIBE = 'punsubscribe';
    const MESSAGE      = 'message';
    const PMESSAGE     = 'pmessage';

    const STATUS_VALID       = 1;	// 0b0001
    const STATUS_SUBSCRIBED  = 2;	// 0b0010
    const STATUS_PSUBSCRIBED = 4;	// 0b0100

    private $position = null;
    private $statusFlags = self::STATUS_VALID;

    /**
     * Automatically closes the context when PHP's garbage collector kicks in.
     */
    public function __destruct()
    {
        $this->closeContext(true);
    }

    /**
     * Checks if the specified flag is valid in the state of the context.
     *
     * @param int $value Flag.
     * @return Boolean
     */
    protected function isFlagSet($value)
    {
        return ($this->statusFlags & $value) === $value;
    }

    /**
     * Subscribes to the specified channels.
     *
     * @param mixed $arg,... One or more channel names.
     */
    public function subscribe(/* arguments */)
    {
        $this->writeCommand(self::SUBSCRIBE, func_get_args());
        $this->statusFlags |= self::STATUS_SUBSCRIBED;
    }

    /**
     * Unsubscribes from the specified channels.
     *
     * @param mixed $arg,... One or more channel names.
     */
    public function unsubscribe(/* arguments */)
    {
        $this->writeCommand(self::UNSUBSCRIBE, func_get_args());
    }

    /**
     * Subscribes to the specified channels using a pattern.
     *
     * @param mixed $arg,... One or more channel name patterns.
     */
    public function psubscribe(/* arguments */)
    {
        $this->writeCommand(self::PSUBSCRIBE, func_get_args());
        $this->statusFlags |= self::STATUS_PSUBSCRIBED;
    }

    /**
     * Unsubscribes from the specified channels using a pattern.
     *
     * @param mixed $arg,... One or more channel name patterns.
     */
    public function punsubscribe(/* arguments */)
    {
        $this->writeCommand(self::PUNSUBSCRIBE, func_get_args());
    }

    /**
     * Closes the context by unsubscribing from all the subscribed channels.
     * Optionally, the context can be forcefully closed by dropping the
     * underlying connection.
     *
     * @param Boolean $force Forcefully close the context by closing the connection.
     * @return Boolean Returns false if there are no pending messages.
     */
    public function closeContext($force = false)
    {
        if (!$this->valid()) {
            return false;
        }

        if ($force) {
            $this->invalidate();
            $this->disconnect();
        } else {
            if ($this->isFlagSet(self::STATUS_SUBSCRIBED)) {
                $this->unsubscribe();
            }
            if ($this->isFlagSet(self::STATUS_PSUBSCRIBED)) {
                $this->punsubscribe();
            }
        }

        return !$force;
    }

    /**
     * Closes the underlying connection on forced disconnection.
     */
    protected abstract function disconnect();

    /**
     * Writes a Redis command on the underlying connection.
     *
     * @param string $method ID of the command.
     * @param array $arguments List of arguments.
     */
    protected abstract function writeCommand($method, $arguments);

    /**
     * {@inheritdoc}
     */
    public function rewind()
    {
        // NOOP
    }

    /**
     * Returns the last message payload retrieved from the server and generated
     * by one of the active subscriptions.
     *
     * @return array
     */
    public function current()
    {
        return $this->getValue();
    }

    /**
     * {@inheritdoc}
     */
    public function key()
    {
        return $this->position;
    }

    /**
     * {@inheritdoc}
     */
    public function next()
    {
        if ($this->valid()) {
            $this->position++;
        }

        return $this->position;
    }

    /**
     * Checks if the the context is still in a valid state to continue.
     *
     * @return Boolean
     */
    public function valid()
    {
        $isValid = $this->isFlagSet(self::STATUS_VALID);
        $subscriptionFlags = self::STATUS_SUBSCRIBED | self::STATUS_PSUBSCRIBED;
        $hasSubscriptions = ($this->statusFlags & $subscriptionFlags) > 0;

        return $isValid && $hasSubscriptions;
    }

    /**
     * Resets the state of the context.
     */
    protected function invalidate()
    {
        $this->statusFlags = 0;	// 0b0000;
    }

    /**
     * Waits for a new message from the server generated by one of the active
     * subscriptions and returns it when available.
     *
     * @return array
     */
    protected abstract function getValue();
}

/**
 * Client-side abstraction of a Publish / Subscribe context.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class PubSubContext extends AbstractPubSubContext
{
    private $client;
    private $options;

    /**
     * @param ClientInterface $client Client instance used by the context.
     * @param array $options Options for the context initialization.
     */
    public function __construct(ClientInterface $client, Array $options = null)
    {
        $this->checkCapabilities($client);
        $this->options = $options ?: array();
        $this->client = $client;

        $this->genericSubscribeInit('subscribe');
        $this->genericSubscribeInit('psubscribe');
    }

    /**
     * Returns the underlying client instance used by the pub/sub iterator.
     *
     * @return ClientInterface
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Checks if the passed client instance satisfies the required conditions
     * needed to initialize a Publish / Subscribe context.
     *
     * @param ClientInterface $client Client instance used by the context.
     */
    private function checkCapabilities(ClientInterface $client)
    {
        if ($client->getConnection() instanceof AggregatedConnectionInterface) {
            throw new NotSupportedException('Cannot initialize a PUB/SUB context when using aggregated connections');
        }

        $commands = array('publish', 'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe');

        if ($client->getProfile()->supportsCommands($commands) === false) {
            throw new NotSupportedException('The current profile does not support PUB/SUB related commands');
        }
    }

    /**
     * This method shares the logic to handle both SUBSCRIBE and PSUBSCRIBE.
     *
     * @param string $subscribeAction Type of subscription.
     */
    private function genericSubscribeInit($subscribeAction)
    {
        if (isset($this->options[$subscribeAction])) {
            $this->$subscribeAction($this->options[$subscribeAction]);
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function writeCommand($method, $arguments)
    {
        $arguments = Command::normalizeArguments($arguments);
        $command = $this->client->createCommand($method, $arguments);
        $this->client->getConnection()->writeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    protected function disconnect()
    {
        $this->client->disconnect();
    }

    /**
     * {@inheritdoc}
     */
    protected function getValue()
    {
        $response = $this->client->getConnection()->read();

        switch ($response[0]) {
            case self::SUBSCRIBE:
            case self::UNSUBSCRIBE:
            case self::PSUBSCRIBE:
            case self::PUNSUBSCRIBE:
                if ($response[2] === 0) {
                    $this->invalidate();
                }

            case self::MESSAGE:
                return (object) array(
                    'kind'    => $response[0],
                    'channel' => $response[1],
                    'payload' => $response[2],
                );

            case self::PMESSAGE:
                return (object) array(
                    'kind'    => $response[0],
                    'pattern' => $response[1],
                    'channel' => $response[2],
                    'payload' => $response[3],
                );

            default:
                $message = "Received an unknown message type {$response[0]} inside of a pubsub context";
                throw new ClientException($message);
        }
    }
}

/**
 * Method-dispatcher loop built around the client-side abstraction of a Redis
 * Publish / Subscribe context.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class DispatcherLoop
{
    private $pubSubContext;

    protected $callbacks;
    protected $defaultCallback;
    protected $subscriptionCallback;

    /**
     * @param ClientInterface $client Client instance used by the context.
     */
    public function __construct(ClientInterface $client)
    {
        $this->callbacks = array();
        $this->pubSubContext = $client->pubSub();
    }

    /**
     * Checks if the passed argument is a valid callback.
     *
     * @param mixed $callable A callback.
     */
    protected function validateCallback($callable)
    {
        if (!is_callable($callable)) {
            throw new \InvalidArgumentException("A valid callable object must be provided");
        }
    }

    /**
     * Returns the underlying Publish / Subscribe context.
     *
     * @return PubSubContext
     */
    public function getPubSubContext()
    {
        return $this->pubSubContext;
    }

    /**
     * Sets a callback that gets invoked upon new subscriptions.
     *
     * @param mixed $callable A callback.
     */
    public function subscriptionCallback($callable = null)
    {
        if (isset($callable)) {
            $this->validateCallback($callable);
        }

        $this->subscriptionCallback = $callable;
    }

    /**
     * Sets a callback that gets invoked when a message is received on a
     * channel that does not have an associated callback.
     *
     * @param mixed $callable A callback.
     */
    public function defaultCallback($callable = null)
    {
        if (isset($callable)) {
            $this->validateCallback($callable);
        }

        $this->subscriptionCallback = $callable;
    }

    /**
     * Binds a callback to a channel.
     *
     * @param string $channel Channel name.
     * @param Callable $callback A callback.
     */
    public function attachCallback($channel, $callback)
    {
        $callbackName = $this->getPrefixKeys() . $channel;

        $this->validateCallback($callback);
        $this->callbacks[$callbackName] = $callback;
        $this->pubSubContext->subscribe($channel);
    }

    /**
     * Stops listening to a channel and removes the associated callback.
     *
     * @param string $channel Redis channel.
     */
    public function detachCallback($channel)
    {
        $callbackName = $this->getPrefixKeys() . $channel;

        if (isset($this->callbacks[$callbackName])) {
            unset($this->callbacks[$callbackName]);
            $this->pubSubContext->unsubscribe($channel);
        }
    }

    /**
     * Starts the dispatcher loop.
     */
    public function run()
    {
        foreach ($this->pubSubContext as $message) {
            $kind = $message->kind;

            if ($kind !== PubSubContext::MESSAGE && $kind !== PubSubContext::PMESSAGE) {
                if (isset($this->subscriptionCallback)) {
                    $callback = $this->subscriptionCallback;
                    call_user_func($callback, $message);
                }

                continue;
            }

            if (isset($this->callbacks[$message->channel])) {
                $callback = $this->callbacks[$message->channel];
                call_user_func($callback, $message->payload);
            } else if (isset($this->defaultCallback)) {
                $callback = $this->defaultCallback;
                call_user_func($callback, $message);
            }
        }
    }

    /**
     * Terminates the dispatcher loop.
     */
    public function stop()
    {
        $this->pubSubContext->closeContext();
    }

    /**
     * Return the prefix of the keys
     *
     * @return string
     */
    protected function getPrefixKeys()
    {
        $options = $this->pubSubContext->getClient()->getOptions();

        if (isset($options->prefix)) {
            return $options->prefix->getPrefix();
        }

        return '';
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Transaction;

use Predis\PredisException;
use SplQueue;
use Predis\BasicClientInterface;
use Predis\ClientException;
use Predis\ClientInterface;
use Predis\CommunicationException;
use Predis\ExecutableContextInterface;
use Predis\NotSupportedException;
use Predis\ResponseErrorInterface;
use Predis\ResponseQueued;
use Predis\ServerException;
use Predis\Command\CommandInterface;
use Predis\Connection\AggregatedConnectionInterface;
use Predis\Protocol\ProtocolException;

/**
 * Client-side abstraction of a Redis transaction based on MULTI / EXEC.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MultiExecContext implements BasicClientInterface, ExecutableContextInterface
{
    const STATE_RESET       = 0;    // 0b00000
    const STATE_INITIALIZED = 1;    // 0b00001
    const STATE_INSIDEBLOCK = 2;    // 0b00010
    const STATE_DISCARDED   = 4;    // 0b00100
    const STATE_CAS         = 8;    // 0b01000
    const STATE_WATCH       = 16;   // 0b10000

    private $state;
    private $canWatch;

    protected $client;
    protected $options;
    protected $commands;

    /**
     * @param ClientInterface $client Client instance used by the context.
     * @param array $options Options for the context initialization.
     */
    public function __construct(ClientInterface $client, Array $options = null)
    {
        $this->checkCapabilities($client);
        $this->options = $options ?: array();
        $this->client = $client;
        $this->reset();
    }

    /**
     * Sets the internal state flags.
     *
     * @param int $flags Set of flags
     */
    protected function setState($flags)
    {
        $this->state = $flags;
    }

    /**
     * Gets the internal state flags.
     *
     * @return int
     */
    protected function getState()
    {
        return $this->state;
    }

    /**
     * Sets one or more flags.
     *
     * @param int $flags Set of flags
     */
    protected function flagState($flags)
    {
        $this->state |= $flags;
    }

    /**
     * Resets one or more flags.
     *
     * @param int $flags Set of flags
     */
    protected function unflagState($flags)
    {
        $this->state &= ~$flags;
    }

    /**
     * Checks is a flag is set.
     *
     * @param int $flags Flag
     * @return Boolean
     */
    protected function checkState($flags)
    {
        return ($this->state & $flags) === $flags;
    }

    /**
     * Checks if the passed client instance satisfies the required conditions
     * needed to initialize a transaction context.
     *
     * @param ClientInterface $client Client instance used by the context.
     */
    private function checkCapabilities(ClientInterface $client)
    {
        if ($client->getConnection() instanceof AggregatedConnectionInterface) {
            throw new NotSupportedException('Cannot initialize a MULTI/EXEC context when using aggregated connections');
        }

        $profile = $client->getProfile();

        if ($profile->supportsCommands(array('multi', 'exec', 'discard')) === false) {
            throw new NotSupportedException('The current profile does not support MULTI, EXEC and DISCARD');
        }

        $this->canWatch = $profile->supportsCommands(array('watch', 'unwatch'));
    }

    /**
     * Checks if WATCH and UNWATCH are supported by the server profile.
     */
    private function isWatchSupported()
    {
        if ($this->canWatch === false) {
            throw new NotSupportedException('The current profile does not support WATCH and UNWATCH');
        }
    }

    /**
     * Resets the state of a transaction.
     */
    protected function reset()
    {
        $this->setState(self::STATE_RESET);
        $this->commands = new SplQueue();
    }

    /**
     * Initializes a new transaction.
     */
    protected function initialize()
    {
        if ($this->checkState(self::STATE_INITIALIZED)) {
            return;
        }

        $options = $this->options;

        if (isset($options['cas']) && $options['cas']) {
            $this->flagState(self::STATE_CAS);
        }
        if (isset($options['watch'])) {
            $this->watch($options['watch']);
        }

        $cas = $this->checkState(self::STATE_CAS);
        $discarded = $this->checkState(self::STATE_DISCARDED);

        if (!$cas || ($cas && $discarded)) {
            $this->client->multi();

            if ($discarded) {
                $this->unflagState(self::STATE_CAS);
            }
        }

        $this->unflagState(self::STATE_DISCARDED);
        $this->flagState(self::STATE_INITIALIZED);
    }

    /**
     * Dynamically invokes a Redis command with the specified arguments.
     *
     * @param string $method Command ID.
     * @param array $arguments Arguments for the command.
     * @return mixed
     */
    public function __call($method, $arguments)
    {
        $command = $this->client->createCommand($method, $arguments);
        $response = $this->executeCommand($command);

        return $response;
    }

    /**
     * Executes the specified Redis command.
     *
     * @param CommandInterface $command A Redis command.
     * @return mixed
     */
    public function executeCommand(CommandInterface $command)
    {
        $this->initialize();
        $response = $this->client->executeCommand($command);

        if ($this->checkState(self::STATE_CAS)) {
            return $response;
        }

        if (!$response instanceof ResponseQueued) {
            $this->onProtocolError('The server did not respond with a QUEUED status reply');
        }

        $this->commands->enqueue($command);

        return $this;
    }

    /**
     * Executes WATCH on one or more keys.
     *
     * @param string|array $keys One or more keys.
     * @return mixed
     */
    public function watch($keys)
    {
        $this->isWatchSupported();

        if ($this->checkState(self::STATE_INITIALIZED) && !$this->checkState(self::STATE_CAS)) {
            throw new ClientException('WATCH after MULTI is not allowed');
        }

        $reply = $this->client->watch($keys);
        $this->flagState(self::STATE_WATCH);

        return $reply;
    }

    /**
     * Finalizes the transaction on the server by executing MULTI on the server.
     *
     * @return MultiExecContext
     */
    public function multi()
    {
        if ($this->checkState(self::STATE_INITIALIZED | self::STATE_CAS)) {
            $this->unflagState(self::STATE_CAS);
            $this->client->multi();
        } else {
            $this->initialize();
        }

        return $this;
    }

    /**
     * Executes UNWATCH.
     *
     * @return MultiExecContext
     */
    public function unwatch()
    {
        $this->isWatchSupported();
        $this->unflagState(self::STATE_WATCH);
        $this->__call('unwatch', array());

        return $this;
    }

    /**
     * Resets a transaction by UNWATCHing the keys that are being WATCHed and
     * DISCARDing the pending commands that have been already sent to the server.
     *
     * @return MultiExecContext
     */
    public function discard()
    {
        if ($this->checkState(self::STATE_INITIALIZED)) {
            $command = $this->checkState(self::STATE_CAS) ? 'unwatch' : 'discard';
            $this->client->$command();
            $this->reset();
            $this->flagState(self::STATE_DISCARDED);
        }

        return $this;
    }

    /**
     * Executes the whole transaction.
     *
     * @return mixed
     */
    public function exec()
    {
        return $this->execute();
    }

    /**
     * Checks the state of the transaction before execution.
     *
     * @param mixed $callable Callback for execution.
     */
    private function checkBeforeExecution($callable)
    {
        if ($this->checkState(self::STATE_INSIDEBLOCK)) {
            throw new ClientException("Cannot invoke 'execute' or 'exec' inside an active client transaction block");
        }

        if ($callable) {
            if (!is_callable($callable)) {
                throw new \InvalidArgumentException('Argument passed must be a callable object');
            }

            if (!$this->commands->isEmpty()) {
                $this->discard();
                throw new ClientException('Cannot execute a transaction block after using fluent interface');
            }
        }

        if (isset($this->options['retry']) && !isset($callable)) {
            $this->discard();
            throw new \InvalidArgumentException('Automatic retries can be used only when a transaction block is provided');
        }
    }

    /**
     * Handles the actual execution of the whole transaction.
     *
     * @param mixed $callable Optional callback for execution.
     * @return array
     */
    public function execute($callable = null)
    {
        $this->checkBeforeExecution($callable);

        $reply = null;
        $values = array();
        $attempts = isset($this->options['retry']) ? (int) $this->options['retry'] : 0;

        do {
            if ($callable !== null) {
                $this->executeTransactionBlock($callable);
            }

            if ($this->commands->isEmpty()) {
                if ($this->checkState(self::STATE_WATCH)) {
                    $this->discard();
                }

                return;
            }

            $reply = $this->client->exec();

            if ($reply === null) {
                if ($attempts === 0) {
                    $message = 'The current transaction has been aborted by the server';
                    throw new AbortedMultiExecException($this, $message);
                }

                $this->reset();

                if (isset($this->options['on_retry']) && is_callable($this->options['on_retry'])) {
                    call_user_func($this->options['on_retry'], $this, $attempts);
                }

                continue;
            }

            break;
        } while ($attempts-- > 0);

        $exec = $reply instanceof \Iterator ? iterator_to_array($reply) : $reply;
        $commands = $this->commands;

        $size = count($exec);
        if ($size !== count($commands)) {
            $this->onProtocolError("EXEC returned an unexpected number of replies");
        }

        $clientOpts = $this->client->getOptions();
        $useExceptions = isset($clientOpts->exceptions) ? $clientOpts->exceptions : true;

        for ($i = 0; $i < $size; $i++) {
            $commandReply = $exec[$i];

            if ($commandReply instanceof ResponseErrorInterface && $useExceptions) {
                $message = $commandReply->getMessage();
                throw new ServerException($message);
            }

            if ($commandReply instanceof \Iterator) {
                $commandReply = iterator_to_array($commandReply);
            }

            $values[$i] = $commands->dequeue()->parseResponse($commandReply);
        }

        return $values;
    }

    /**
     * Passes the current transaction context to a callable block for execution.
     *
     * @param mixed $callable Callback.
     */
    protected function executeTransactionBlock($callable)
    {
        $blockException = null;
        $this->flagState(self::STATE_INSIDEBLOCK);

        try {
            call_user_func($callable, $this);
        } catch (CommunicationException $exception) {
            $blockException = $exception;
        } catch (ServerException $exception) {
            $blockException = $exception;
        } catch (\Exception $exception) {
            $blockException = $exception;
            $this->discard();
        }

        $this->unflagState(self::STATE_INSIDEBLOCK);

        if ($blockException !== null) {
            throw $blockException;
        }
    }

    /**
     * Helper method that handles protocol errors encountered inside a transaction.
     *
     * @param string $message Error message.
     */
    private function onProtocolError($message)
    {
        // Since a MULTI/EXEC block cannot be initialized when using aggregated
        // connections, we can safely assume that Predis\Client::getConnection()
        // will always return an instance of Predis\Connection\SingleConnectionInterface.
        CommunicationException::handle(new ProtocolException(
            $this->client->getConnection(), $message
        ));
    }
}

/**
 * Exception class that identifies MULTI / EXEC transactions aborted by Redis.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class AbortedMultiExecException extends PredisException
{
    private $transaction;

    /**
     * @param MultiExecContext $transaction Transaction that generated the exception.
     * @param string $message Error message.
     * @param int $code Error code.
     */
    public function __construct(MultiExecContext $transaction, $message, $code = null)
    {
        parent::__construct($message, $code);
        $this->transaction = $transaction;
    }

    /**
     * Returns the transaction that generated the exception.
     *
     * @return MultiExecContext
     */
    public function getTransaction()
    {
        return $this->transaction;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Session;

use SessionHandlerInterface;
use Predis\ClientInterface;

/**
 * Session handler class that relies on Predis\Client to store PHP's sessions
 * data into one or multiple Redis servers.
 *
 * This class is mostly intended for PHP 5.4 but it can be used under PHP 5.3 provided
 * that a polyfill for `SessionHandlerInterface` is defined by either you or an external
 * package such as `symfony/http-foundation`.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SessionHandler implements SessionHandlerInterface
{
    protected $client;
    protected $ttl;

    /**
     * @param ClientInterface $client Fully initialized client instance.
     * @param array $options Session handler options.
     */
    public function __construct(ClientInterface $client, Array $options = array())
    {
        $this->client = $client;
        $this->ttl = (int) (isset($options['gc_maxlifetime']) ? $options['gc_maxlifetime'] : ini_get('session.gc_maxlifetime'));
    }

    /**
     * Registers the handler instance as the current session handler.
     */
    public function register()
    {
        if (version_compare(PHP_VERSION, '5.4.0') >= 0) {
            session_set_save_handler($this, true);
        } else {
            session_set_save_handler(
                array($this, 'open'),
                array($this, 'close'),
                array($this, 'read'),
                array($this, 'write'),
                array($this, 'destroy'),
                array($this, 'gc')
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function open($save_path, $session_id)
    {
        // NOOP

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function close()
    {
        // NOOP

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function gc($maxlifetime)
    {
        // NOOP

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function read($session_id)
    {
        if ($data = $this->client->get($session_id)) {
            return $data;
        }

        return '';
    }
    /**
     * {@inheritdoc}
     */
    public function write($session_id, $session_data)
    {
        $this->client->setex($session_id, $this->ttl, $session_data);

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function destroy($session_id)
    {
        $this->client->del($session_id);

        return true;
    }

    /**
     * Returns the underlying client instance.
     *
     * @return ClientInterface
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Returns the session max lifetime value.
     *
     * @return int
     */
    public function getMaxLifeTime()
    {
        return $this->ttl;
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Monitor;

use Predis\ClientInterface;
use Predis\NotSupportedException;
use Predis\Connection\AggregatedConnectionInterface;

/**
 * Client-side abstraction of a Redis MONITOR context.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class MonitorContext implements \Iterator
{
    private $client;
    private $isValid;
    private $position;

    /**
     * @param ClientInterface $client Client instance used by the context.
     */
    public function __construct(ClientInterface $client)
    {
        $this->checkCapabilities($client);
        $this->client = $client;
        $this->openContext();
    }

    /**
     * Automatically closes the context when PHP's garbage collector kicks in.
     */
    public function __destruct()
    {
        $this->closeContext();
    }

    /**
     * Checks if the passed client instance satisfies the required conditions
     * needed to initialize a monitor context.
     *
     * @param ClientInterface $client Client instance used by the context.
     */
    private function checkCapabilities(ClientInterface $client)
    {
        if ($client->getConnection() instanceof AggregatedConnectionInterface) {
            throw new NotSupportedException('Cannot initialize a monitor context when using aggregated connections');
        }
        if ($client->getProfile()->supportsCommand('monitor') === false) {
            throw new NotSupportedException('The current profile does not support the MONITOR command');
        }
    }

    /**
     * Initializes the context and sends the MONITOR command to the server.
     */
    protected function openContext()
    {
        $this->isValid = true;
        $monitor = $this->client->createCommand('monitor');
        $this->client->executeCommand($monitor);
    }

    /**
     * Closes the context. Internally this is done by disconnecting from server
     * since there is no way to terminate the stream initialized by MONITOR.
     */
    public function closeContext()
    {
        $this->client->disconnect();
        $this->isValid = false;
    }

    /**
     * {@inheritdoc}
     */
    public function rewind()
    {
        // NOOP
    }

    /**
     * Returns the last message payload retrieved from the server.
     *
     * @return Object
     */
    public function current()
    {
        return $this->getValue();
    }

    /**
     * {@inheritdoc}
     */
    public function key()
    {
        return $this->position;
    }

    /**
     * {@inheritdoc}
     */
    public function next()
    {
        $this->position++;
    }

    /**
     * Checks if the the context is still in a valid state to continue.
     *
     * @return Boolean
     */
    public function valid()
    {
        return $this->isValid;
    }

    /**
     * Waits for a new message from the server generated by MONITOR and
     * returns it when available.
     *
     * @return Object
     */
    private function getValue()
    {
        $database = 0;
        $client = null;
        $event = $this->client->getConnection()->read();

        $callback = function ($matches) use (&$database, &$client) {
            if (2 === $count = count($matches)) {
                // Redis <= 2.4
                $database = (int) $matches[1];
            }

            if (4 === $count) {
                // Redis >= 2.6
                $database = (int) $matches[2];
                $client = $matches[3];
            }

            return ' ';
        };

        $event = preg_replace_callback('/ \(db (\d+)\) | \[(\d+) (.*?)\] /', $callback, $event, 1);
        @list($timestamp, $command, $arguments) = explode(' ', $event, 3);

        return (object) array(
            'timestamp' => (float) $timestamp,
            'database'  => $database,
            'client'    => $client,
            'command'   => substr($command, 1, -1),
            'arguments' => $arguments,
        );
    }
}

/* --------------------------------------------------------------------------- */

namespace Predis\Replication;

use Predis\NotSupportedException;
use Predis\Command\CommandInterface;

/**
 * Defines a strategy for master/reply replication.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class ReplicationStrategy
{
    protected $disallowed;
    protected $readonly;
    protected $readonlySHA1;

    /**
     *
     */
    public function __construct()
    {
        $this->disallowed = $this->getDisallowedOperations();
        $this->readonly = $this->getReadOnlyOperations();
        $this->readonlySHA1 = array();
    }

    /**
     * Returns if the specified command performs a read-only operation
     * against a key stored on Redis.
     *
     * @param CommandInterface $command Instance of Redis command.
     * @return Boolean
     */
    public function isReadOperation(CommandInterface $command)
    {
        if (isset($this->disallowed[$id = $command->getId()])) {
            throw new NotSupportedException("The command $id is not allowed in replication mode");
        }

        if (isset($this->readonly[$id])) {
            if (true === $readonly = $this->readonly[$id]) {
                return true;
            }

            return call_user_func($readonly, $command);
        }

        if (($eval = $id === 'EVAL') || $id === 'EVALSHA') {
            $sha1 = $eval ? sha1($command->getArgument(0)) : $command->getArgument(0);

            if (isset($this->readonlySHA1[$sha1])) {
                if (true === $readonly = $this->readonlySHA1[$sha1]) {
                    return true;
                }

                return call_user_func($readonly, $command);
            }
        }

        return false;
    }

    /**
     * Returns if the specified command is disallowed in a master/slave
     * replication context.
     *
     * @param CommandInterface $command Instance of Redis command.
     * @return Boolean
     */
    public function isDisallowedOperation(CommandInterface $command)
    {
        return isset($this->disallowed[$command->getId()]);
    }

    /**
     * Checks if a SORT command is a readable operation by parsing the arguments
     * array of the specified commad instance.
     *
     * @param CommandInterface $command Instance of Redis command.
     * @return Boolean
     */
    protected function isSortReadOnly(CommandInterface $command)
    {
        $arguments = $command->getArguments();
        return ($c = count($arguments)) === 1 ? true : $arguments[$c - 2] !== 'STORE';
    }

    /**
     * Marks a command as a read-only operation. When the behaviour of a
     * command can be decided only at runtime depending on its arguments,
     * a callable object can be provided to dynamically check if the passed
     * instance of a command performs write operations or not.
     *
     * @param string $commandID ID of the command.
     * @param mixed $readonly A boolean or a callable object.
     */
    public function setCommandReadOnly($commandID, $readonly = true)
    {
        $commandID = strtoupper($commandID);

        if ($readonly) {
            $this->readonly[$commandID] = $readonly;
        } else {
            unset($this->readonly[$commandID]);
        }
    }

    /**
     * Marks a Lua script for EVAL and EVALSHA as a read-only operation. When
     * the behaviour of a script can be decided only at runtime depending on
     * its arguments, a callable object can be provided to dynamically check
     * if the passed instance of EVAL or EVALSHA performs write operations or
     * not.
     *
     * @param string $script Body of the Lua script.
     * @param mixed $readonly A boolean or a callable object.
     */
    public function setScriptReadOnly($script, $readonly = true)
    {
        $sha1 = sha1($script);

        if ($readonly) {
            $this->readonlySHA1[$sha1] = $readonly;
        } else {
            unset($this->readonlySHA1[$sha1]);
        }
    }

    /**
     * Returns the default list of disallowed commands.
     *
     * @return array
     */
    protected function getDisallowedOperations()
    {
        return array(
            'SHUTDOWN'          => true,
            'INFO'              => true,
            'DBSIZE'            => true,
            'LASTSAVE'          => true,
            'CONFIG'            => true,
            'MONITOR'           => true,
            'SLAVEOF'           => true,
            'SAVE'              => true,
            'BGSAVE'            => true,
            'BGREWRITEAOF'      => true,
            'SLOWLOG'           => true,
        );
    }

    /**
     * Returns the default list of commands performing read-only operations.
     *
     * @return array
     */
    protected function getReadOnlyOperations()
    {
        return array(
            'EXISTS'            => true,
            'TYPE'              => true,
            'KEYS'              => true,
            'RANDOMKEY'         => true,
            'TTL'               => true,
            'GET'               => true,
            'MGET'              => true,
            'SUBSTR'            => true,
            'STRLEN'            => true,
            'GETRANGE'          => true,
            'GETBIT'            => true,
            'LLEN'              => true,
            'LRANGE'            => true,
            'LINDEX'            => true,
            'SCARD'             => true,
            'SISMEMBER'         => true,
            'SINTER'            => true,
            'SUNION'            => true,
            'SDIFF'             => true,
            'SMEMBERS'          => true,
            'SRANDMEMBER'       => true,
            'ZRANGE'            => true,
            'ZREVRANGE'         => true,
            'ZRANGEBYSCORE'     => true,
            'ZREVRANGEBYSCORE'  => true,
            'ZCARD'             => true,
            'ZSCORE'            => true,
            'ZCOUNT'            => true,
            'ZRANK'             => true,
            'ZREVRANK'          => true,
            'HGET'              => true,
            'HMGET'             => true,
            'HEXISTS'           => true,
            'HLEN'              => true,
            'HKEYS'             => true,
            'HVALS'             => true,
            'HGETALL'           => true,
            'PING'              => true,
            'AUTH'              => true,
            'SELECT'            => true,
            'ECHO'              => true,
            'QUIT'              => true,
            'OBJECT'            => true,
            'BITCOUNT'          => true,
            'TIME'              => true,
            'SORT'              => array($this, 'isSortReadOnly'),
        );
    }
}

/* --------------------------------------------------------------------------- */

