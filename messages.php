<?php
require_once 'config.php';
require_once 'php/Fakebook.php';

$pageTitle = 'Homepage';

$fakebook = new Fakebook();

// if the user is not logged, redirect to the login page
if (!$fakebook->getLoggedUserId()) {
	header("location: index.php");
}

$messages = $fakebook->getMessages();

?>
<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>

	<?php include 'topbar.inc.php'; ?>
	
	<div id="container">
		<?php include 'sidebar.inc.php' ?>
		<section id="content">
			<h1 style="margin-top: 0;">Messages:</h1>
			<?php if (!$messages) {
				echo 'Sorry but there are no messages.';
			}?>
			<?php
			foreach ($messages as $message) {
				$aux = explode('|',$message); ?>
				<div class="message">
					<div class="author"><a href="user.php?id=<?= $aux[0] ?>"><?= $fakebook->getFullName($aux[0]) ?></a></div>
					<div class="message"><?= $aux[1] ?></div>
					<div class="time"><?= $fakebook->timeAgo($aux[2]) ?></div>
					<a href="send-message.php?to=<?= $aux[0] ?>">Reply</a>
				</div>
				<?php } ?>
		</section>
	</div>
</body>
</html>