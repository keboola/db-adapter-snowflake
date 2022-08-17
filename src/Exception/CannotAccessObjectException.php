<?php

declare(strict_types=1);

namespace Keboola\SnowflakeDbAdapter\Exception;

use Exception;

class CannotAccessObjectException extends Exception implements ExceptionInterface
{
}
