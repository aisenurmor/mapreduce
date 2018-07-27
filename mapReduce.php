<?php
function liveExecuteCommand($cmd)
{

    while (@ ob_end_flush()); // end all output buffers if any

    $proc = popen("$cmd 2>&1 ; echo Exit status : $?", 'r');

    $live_output     = "";
    $complete_output = "";

    while (!feof($proc))
    {
        $live_output     = fread($proc, 4096);
        $complete_output = $complete_output . $live_output;
        echo "$live_output";
        @ flush();
    }

    pclose($proc);

    // get exit status
    preg_match('/[0-9]+$/', $complete_output, $matches);

    // return exit status and intended output
    return array (
                    'exit_status'  => intval($matches[0]),
                    'output'       => str_replace("Exit status : " . $matches[0], '', $complete_output)
                 );
}

if($_POST)
{
	$command2=escapeshellcmd('python /var/www/html/mapReduce.py');
}
#$result= liveExecuteCommand($command2);
#if($result['exit_status'] === 0){
#   echo $result;
#} else {
#    echo "hata";
#}

#if($_POST)
#{
#	$command2=escapeshellcmd('python /var/www/html/mapReduce.py');
#	$output2=shell_exec($command2);
#	#echo "<p>$text</p>";
#}
?>
<style>
	form {
  	   text-align: center;
           background-color:E5F2F3;
	   width:50%;
           height:100%;
	   margin:0 auto;
	}
	button {
	   width:80px;
        }
	textarea{
	   text-align: center;
	   padding:10px;
	}
</style>
<form method="post">
    <br><br><button name="btn" type="submit">Get</button><br><br>
    <textarea name="mytextarea" rows="50" cols="90">
	<?php 
		echo "Yukleniyor.....";
		echo "\n\n";	
		liveExecuteCommand($command2); 
	?>
    </textarea>
</form>
