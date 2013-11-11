<div id="topbar">
	<div>
		<span id="logo"><a href="index.php"><?= TITLE ?></a></span>
		<a href="friend-requests.php"><img class="top-button <?= ($topBarSelectedIcon == 1) ? 'selected' : '' ?>" src="img/friend-requests.png"></a>
		<img class="top-button <?= ($topBarSelectedIcon == 2) ? 'selected' : '' ?>" src="img/messages.png">
		<img class="top-button <?= ($topBarSelectedIcon == 3) ? 'selected' : '' ?>" src="img/notifications.png">
		<input id="search" type="text">
	</div>
</div>