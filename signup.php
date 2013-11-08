<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$pageTitle = 'Sign-up';

if (!isset($_POST) || $_POST['email'] == "" || $_POST['password'] == "" || $_POST['name'] == "" || $_POST['surname'] == "") {
	header("location: error.php?code=1");;
}

$fakebook = new Fakebook();
$fakebook->signup($_POST);

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
		You have been registered. You can login from <a href="index.php">here</a>.
	</div>
</body>
</html>