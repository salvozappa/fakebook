<?php 
require_once 'config.php';
$pageTitle = 'Ok';
$code = $_GET['code'];

$messages = array(
	1 => 'Friendship requested',
	2 => 'Status published'
	);
?>

<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>
	<div id="top-bar">
		<div>
			<span id="logo"><a href="index.php"><?= TITLE ?></a></span>
		</div>
	</div>
	<div id="container" style="margin-top: 50px;">
		<h1>Ok</h1>
		<?= $messages[$code]; ?><br><br>
		<a href="javascript:history.back()">Go Back</a><br>
		<a href="home.php">Homepage</a>
	</div>
</body>
</html>