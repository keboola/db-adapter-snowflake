<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Exception;

use RuntimeException as GenericRuntimeException;

class RuntimeException extends GenericRuntimeException implements ExceptionInterface
{

}
