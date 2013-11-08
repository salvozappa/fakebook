<?php
require_once 'config.php';
require_once 'php/Fakebook.php';

$pageTitle = 'Homepage';

$fakebook = new Fakebook();

// if the user is not logged, redirect to the login page
if (!$fakebook->getLoggedUserId()) {
	header("location: index.php");
}

?>
<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>
	<?php include 'top-bar.inc.php'; ?>
	<div id="container">
		<aside id="left-column">
			<p><?= $fakebook->getFullName() ?></p>
			<a href="logout.php">Logout</a>
		</aside>
		<form id="new-status" method="post" action="push-status.php">
			<input type="text" name="message" placeholder="Say something..!">
			<input type="submit">
		</form>
		<section id="content">
			<h1 style="margin-top: 0;">News feed:</h1>
		</section>
	</div>
</body>
</html>