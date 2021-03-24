<?php

namespace AwsLogger\Log\Engine;

use Cake\Log\Engine\BaseLog;
use Aws\Kinesis\KinesisClient;

/**
 * AWS Kineses Data stream for Logging. Writes logs to a log stream
 * based on the level of log it is.
 */
class KinesisLog extends BaseLog
{
    /**
     * Default config for this class
     *
     * - `levels` string or array, levels the engine is interested in
     * - `scopes` string or array, scopes the engine is interested in
     * - `partitionKey` Determines which shard in the stream the data record is assigned to. 
     *    Partition keys are Unicode strings with a maximum length limit of 256 characters for 
     *    each key. Amazon Kinesis Data Streams uses the partition key as input to a hash function 
     *    that maps the partition key and associated data to a specific shard. Specifically, an 
     *    MD5 hash function is used to map partition keys to 128-bit integer values and to map 
     *    associated data records to shards. As a result of this hashing mechanism, all data 
     *    records with the same partition key map to the same shard within the stream.
     * - `streamName` The name of the log stream.
     * - `region` (string, required) Region to connect to. 
     *    @see http://docs.aws.amazon.com/general/latest/gr/rande.html for a list of available 
     *    regions.
     * - `version` (string, required) The version of the webservice to utilize (e.g., 2006-03-01).
     * - `credentials` (Aws\Credentials\CredentialsInterface|array|bool|callable) Specifies the 
     *    credentials used to sign requests. Provide an Aws\Credentials\CredentialsInterface object, 
     *    an associative array of "key", "secret", and an optional "token" key, false to use null 
     *    credentials, or a callable credentials provider used to create credentials or return null. 
     *    @see Aws\Credentials\CredentialProvider for a list of built-in credentials providers. 
     *    If no credentials are provided, the SDK will attempt to load them from the environment.
     * - `dateFormat` PHP date() format.
     * - `fieldSeparator` The separator between fields within the log message.
     *
     * @var array
     */
    protected $_defaultConfig = [
        'levels'         => [],
        'scopes'         => [],
        'dateFormat'     => 'Y-m-d H:i:s',
        'fieldSeparator' => "\t",
        'partitionKey'   => null,
        'streamName'     => null,
        'region'         => 'us-east-1',
        'version'        => 'latest',
        'credentials'    => [
            'key'    => null,
            'secret' => null,
        ],
    ];

    /**
     * Constructor.
     *
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        parent::__construct($config);
    }

    /**
     * Implements writing to log files.
     *
     * @param mixed $level The severity level of the message being written.
     * @param string $message The message you want to log.
     * @param array $context Additional information about the logged message
     * @return void
     * @see Cake\Log\Log::$_levels
     */
    public function log($level, $message, array $context = []): void
    {
        $FS      = $this->getConfig('fieldSeparator');
        $message = $this->_format($message, $context);
        $output  = $this->_getFormattedDate() .$FS. $_SERVER['SERVER_ADDR'] .$FS. ucfirst($level) .$FS. $message . "\n";

        $this->_write($output);
    }
  
    /**
     * Writes a message to AWS Kinesis.
     *
     * @param string $message
     * @return void
     */
    protected function _write(string $message)
    {
        $client    = new KinesisClient(array_intersect_key($this->getConfig(), array_flip(['region', 'version', 'credentials'])));

        $result = $client->putRecord([
            'Data' => $message,
            'PartitionKey' => $this->getConfig('partitionKey'),
            'StreamName' => $this->getConfig('streamName'),
            //'ExplicitHashKey' => '<string>',
            //'SequenceNumberForOrdering' => '<string>',
        ]);
    }
}