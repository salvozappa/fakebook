<?php
require_once 'config.php';
require_once 'php/Fakebook.php';
$pageTitle = 'Welcome';

$fakebook = new Fakebook();

if ($fakebook->isLoggedIn()) {
	header("location: home.php");
}

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
	<div id="container">
		<form id="login" class="index-form" style="float:left" method="post" action="login.php">
			<h1>Login</h1>
			<input type="text" name="email" placeholder="E-Mail">
			<input type="password" name="password" placeholder="Password">
			<input type="submit" class="button">
		</form>
		<form id="signup" class="index-form" method="post" action="signup.php">
			<h1>Signup</h1>
			<input type="text" name="name" placeholder="Name">
			<input type="text" name="surname" placeholder="Surname">
			<input type="text" name="email" placeholder="E-Mail">
			<input type="password" name="password" placeholder="Password">
			<input type="submit" class="button">
		</form>
	</div>
</body>
</html>