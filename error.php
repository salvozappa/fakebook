<?php 
require_once 'config.php';
$pageTitle = 'Error';
$code = $_GET['code'];

$messages = array(
	1 => 'You have to enter all fields',
	2 => 'You have to enter both e-mail and password',
	3 => 'Wrong username or password',
	4 => 'Missing parameters',
	5 => 'Sorry but this user doesn\'t exists :(',
	6 => 'It seems you are friends already!',
	7 => 'Ops, something wrong happened :('
	);
?>

<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>
	<div id="topbar">
		<div>
			<span id="logo"><a href="index.php"><?= TITLE ?></a></span>
		</div>
	</div>
	<div id="container" style="margin-top: 50px;">
		<h1>Error</h1>
		<?= $messages[$code]; ?><br><br>
		<a href="javascript:history.back()">Go Back</a>
	</div>
</body>
</html>