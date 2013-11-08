<?php
require_once 'config.php';
require_once 'php/Fakebook.php';

$pageTitle = 'Homepage';

$fakebook = new Fakebook();

if (!$fakebook->isLoggedIn()) {
	header("location: index.php");
}

?>
<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>
	<div id="top-bar">
		<div>
			<span id="logo"><a href="index.php"><?= TITLE ?></a></span>
			<img class="top-button" src="img/friend-requests.png">
			<img class="top-button" src="img/messages.png">
			<img class="top-button" src="img/notifications.png">
			<input id="search" type="text">
		</div>
	</div>
	<div id="container">
		<aside id="left-column">
			<p><?= $fakebook->getFullName() ?></p>
			<a href="logout.php">Logout</a>
		</aside>
		<section id="content">
			<h1 style="margin-top: 0;">News feed:</h1>
		</section>
	</div>
</body>
</html>