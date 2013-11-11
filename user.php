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

if (!$fakebook->getLoggedUserId()) {
	header("location: index.php");
}

$fullName = $fakebook->getFullName($uid);

$pageTitle = $fullName;

$statuses = $fakebook->getStatuses($uid);

?>
<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>
	<div id="topbar">
		<div>
			<span id="logo"><a href="index.php"><?= TITLE ?></a></span>
			<img class="top-button" src="img/friend-requests.png">
			<img class="top-button" src="img/messages.png">
			<img class="top-button" src="img/notifications.png">
			<input id="search" type="text">
		</div>
	</div>
	<div id="container">
		<aside id="sidebar">
			<a href="logout.php">Logout</a>
		</aside>
		<section id="content">
			<h1 style="margin-top: 0;"><?= $fullName ?></h1>
			<?php if (!$fakebook->isFriend($uid) && $uid != $fakebook->getLoggedUserId()) : ?>
			<div id="request-friendship">
				<p><?= $fullName ?> is not your friend. <a href="request-friendship.php?id=<?= $uid ?>">Request friendship</a></p>
			</div>
		<?php endif; ?>
		<div class="user-feed">
			<?php
			foreach ($statuses as $status) {
				$aux = explode('|',$status); ?>
				<div class="status">
					<div class="author"><a href="user.php?id=<?= $aux[1] ?>"><?= $fakebook->getFullName($aux[1]) ?></a></div>
					<div class="message"><?= $aux[2] ?></div>
					<div class="time"><?= $fakebook->timeAgo($aux[3]) ?></div>
				</div>
				<?php } ?>
			</div>
		</section>
	</div>
</body>
</html>