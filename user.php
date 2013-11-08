<?php
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

if (!isset($_GET['id'])) {
	$fakebook->error(4);	// Missing parameters
}

$uid = $_GET['id'];

if (!$fakebook->userExists($uid)) {
	$fakebook->error(5);	// User not found
}

if (!$fakebook->isLoggedIn()) {
	header("location: index.php");
}

$fullName = $fakebook->getFullName($uid);

$pageTitle = $fullName;

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
			<a href="logout.php">Logout</a>
		</aside>
		<section id="content">
			<h1 style="margin-top: 0;"><?= $fullName ?></h1>
			<?php if (!$fakebook->isFriend($uid)) : ?>
			<p><?= $fullName ?> is not your friend.</p>
			<a href="request-friendship.php?id=<?= $uid ?>">Request friendship</a>
		<?php endif; ?>
	</section>
</div>
</body>
</html>