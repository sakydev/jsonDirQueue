<?php

//
// Usage:
// 1. Create a directory with sub-folders $DIR/pending and $DIR/ready
//      $dirQueue = new DirQueue($DIR);
// 2. Enqueue + close:
//       $dirQueue->startEnqueue();  // when done enqueuing
//       $dirQueue->enqueue(json_encode($arr1));
//       $dirQueue->enqueue(json_encode($arr2));
//       $dirQueue->enqueue(json_encode($arr3));
//       $dirQueue->stopEnqueue();  // when done enqueuing
// 3. $dirQueue->dequeue(3); // number of entries to dequeue at once
//
ini_set('memory_limit', -1);
class DirQueue {
  static $MAX_RECORDS_PER_FILE = 1000;

	var $dirPath;
  var $threadId;
  var $outputFileName;
  var $outputFile;
  var $outputCount;
  var $outputFileCount;
  var $inputFilePosition;

	function DirQueue($dirPath) {
		$this->dirPath = $dirPath;
    $this->threadId = rand(0, getrandmax());
    $this->outputFileCount = 0;
	}

  function startEnqueue() {
    $this->outputCount = 0;
    $this->outputFileName = $this->threadId . '-' . ($this->outputFileCount++) . '-' . time() . '.txt';
    $this->outputFile = fopen($this->dirPath . '/pending/' . $this->outputFileName, 'a+');
    flock($this->outputFile, LOCK_EX) or die('Failed to get lock: ' . $this->outputFileName);
    // print 'WRITE FILE: ' . $this->dirPath . '/pending/' . $this->outputFileName . "\n";
  }

  function enqueue($record) {
    // saved record in the format of: string, 4 bytes of string length, line separator
    $ret = false;
    if ($record) {
      $line = '';
      if (is_string($record)){
        $line = $record . pack('N', strlen($record)) . "\n";
      } else {
        // jsonify
        $str = json_encode($record);
        $line = $str . pack('N', strlen($str)) . "\n";
      }
      fwrite($this->outputFile, $line);
      $this->outputCount++;
      if ($this->outputCount >= self::$MAX_RECORDS_PER_FILE) { // start writing to a new output file
        $this->stopEnqueue();
        $this->startEnqueue();
      }
      $ret = true;
    }
    return $ret;
  }

  function stopEnqueue() {
    fclose($this->outputFile);  // releases lock
    if ($this->outputCount == 0) {
      // unlink the unnecessary empty file
      @unlink($this->outputFileName);
    } else {
      rename($this->dirPath . '/pending/' . $this->outputFileName, $this->dirPath . '/ready/' . $this->outputFileName);
    }

  }

  function dequeue($total = 0) {
    if ($total <= 0) {
      print 'ERROR: must call dequeue(number)';
    }
    $count = 0;
    $outValues = array();
    if ($dirHandle = opendir($this->dirPath . '/ready/')) {
      # displayMessage('Opened ' . $this->dirPath);
      // echo sprintf("opendir elapsed = %s\n", $opentime - $start);
      while (false !== ($fileName = readdir($dirHandle))) {
        // echo sprintf("readdir elapsed = %s\n", $readdirTime - $opentime);
        if ($fileName == '.' || $fileName == '..') continue;
        $filePath = $this->dirPath . '/ready/' . $fileName;
        # print "READ FILE: " . $filePath . "\n";
        $file = @fopen($this->dirPath . '/ready/' . $fileName, 'r+');
        if ($file && flock($file, LOCK_EX | LOCK_NB)) {
          # echo 'FIle opened';
          $this->inputFilePosition = -1; // initial state
          # displayMessage("Reading last line for $filePath");
          while (($line = $this->_readLastLine($file))) {
            # displayMessage("reading: $line");
            $outValues []= $line;
            $count++;
            if ($count >= $total) {
              break;
            }
            if ($this->inputFilePosition <= 5) {
              break;
            }
          }
          ftruncate($file, $this->inputFilePosition); // truncate file at current position!
          fclose($file);
          if ($this->inputFilePosition <= 5) {  // no more lines in file: delete
            // print "DELETE FILE: " . $filePath . "\n";
            @unlink($filePath);
          }
          if ($count >= $total) {
            break;
          }
        } else {
          # echo 'unable to open file';
        }
      }
      closedir($dirHandle);
    }
    return $outValues;
  }

  function _readLastLine($file) {
    $line = null;
    if ($file) {
      $line = '';
      if ($this->inputFilePosition == -1) {
        fseek($file, 0, SEEK_END);  // start at end; skip last \n
      }
      fseek($file, -5, SEEK_CUR); // skip 4 bytes length and 1 byte separator
      $tmp = fread($file, 5);
      fseek($file, -5, SEEK_CUR);
      if (strlen($tmp) == 5 && substr($tmp, 4, 1) == "\n") {
        $length = unpack("Nint", substr($tmp, 0, 4))['int'];
        // read data
        fseek($file, 0 - $length, SEEK_CUR);
        $line = fread($file, $length);
        if (strlen($line) != $length) {
          $line = null;
        }
        fseek($file, 0 - $length, SEEK_CUR);
      }
    }
    $this->inputFilePosition = ftell($file);
    return $line;
  }
}

?>
