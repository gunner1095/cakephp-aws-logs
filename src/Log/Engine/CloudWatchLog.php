<?php

namespace AwsLogger\Log\Engine;

use Cake\Log\Engine\BaseLog;
use Aws\CloudWatchLogs\CloudWatchLogsClient;
use Aws\CloudWatchLogs\Exception\CloudWatchLogsException;

/**
 * AWS CloudWatchLog stream for Logging. Writes logs to a log stream
 * based on the level of log it is.
 * 
 * The logs must satisfy the following constraints:
 * 
 * - The maximum message size is 1,048,576 bytes.
 * - There is a quota of 5 requests per second per log stream. Additional 
 *   requests are throttled. This quota can't be changed.
 */
class CloudWatchLog extends BaseLog
{
    /**
     * Default config for this class
     *
     * - `levels` string or array, levels the engine is interested in
     * - `scopes` string or array, scopes the engine is interested in
     * - `groupName` The name of the log group.
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
     * - `fieldSeparator` The separator between fields within the log message.
     *
     * @var array
     */
    protected $_defaultConfig = [
        'levels'        => [],
        'scopes'        => [],
        'fieldSeparator' => "\t",
        'groupName'     => null,
        'streamName'    => null,
        'region'        => 'us-east-1',
        'version'       => 'latest',
        'credentials'   => [
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
        $output  = $_SERVER['SERVER_ADDR'] .$FS. ucfirst($level) .$FS. $message . "\n";

        $this->_write($output);
    }
  
    /**
     * Writes a message to CloudWatchLogs.
     *
     * @param string $message
     * @return void
     */
    protected function _write(string $message)
    {
        $client    = new CloudWatchLogsClient(array_intersect_key($this->getConfig(), array_flip(['region', 'version', 'credentials'])));
        $groupName = $this->getConfig('groupName');
        $stramName = $this->getConfig('streamName');
        $timestamp = round(microtime(true) * 1000);

        // Running putLogEvents without a sequence token will fail all but the first time.
        try {
            $client->putLogEvents([
                'logGroupName' => $groupName,
                'logStreamName' => $stramName,
                'logEvents' => [
                    [
                        'timestamp' => $timestamp,
                        'message' => $message
                    ],
                ],
            ]);

        // We catch the error and evaluate the response to get the expected sequence token and try again.
        } catch (CloudWatchLogsException $e) {

            if($e->getAwsErrorCode() === 'InvalidSequenceTokenException') {
                $client->putLogEvents([
                    'logGroupName' => $groupName,
                    'logStreamName' => $stramName,
                    'logEvents' => [
                        [
                            'timestamp' => $timestamp,
                            'message' => $message
                        ],
                    ],
                    'sequenceToken' => $e->get('expectedSequenceToken'),
                ]);
            } else {
                throw $e;
            }
        }
    }
}