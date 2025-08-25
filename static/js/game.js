document.addEventListener('DOMContentLoaded', () => {

    // --- 0. Game Configuration ---
    let config = {};

    // --- 1. DOM Element References ---
    const screens = {
        welcome: document.getElementById('welcome-screen'),
        game: document.getElementById('game-screen'),
        gameOver: document.getElementById('game-over-screen'),
        exit: document.getElementById('exit-screen'),
    };
    const buttons = {
        start: document.getElementById('start-button'),
        cheat: document.getElementById('cheat-button'),
        saveScore: document.getElementById('save-score-button'),
        skipScore: document.getElementById('skip-score-button'),
        quit: document.getElementById('quit-button'),
        quitConfirmYes: document.getElementById('quit-confirm-yes'),
        quitConfirmNo: document.getElementById('quit-confirm-no'),
        playAgain: document.getElementById('play-again-button'),
        playAgainExit: document.getElementById('play-again-exit-button'),
        soundToggle: document.getElementById('sound-toggle'),
    };
	
	// in static/js/game.js

document.addEventListener('DOMContentLoaded', () => {

    // ... (your existing DOM elements and buttons objects) ...

    // --- NEW: Set Initial Focus with a slight delay ---
    // The timeout gives the WebView a moment to render before we set focus.
    setTimeout(() => {
        buttons.start.focus();
    }, 100); // 100 milliseconds
    // ----------------------------------------------------


    // ... (rest of your script) ...

});
    const displays = {
        timeLeft: document.getElementById('time-left'),
        currentScore: document.getElementById('current-score'),
        finalScore: document.getElementById('final-score'),
        exitScore: document.getElementById('exit-score'),
        questionImage: document.getElementById('question-image'),
        answerGrid: document.getElementById('answer-grid'),
        feedbackMessage: document.getElementById('feedback-message'),
        summaryDetails: document.getElementById('summary-details'),
        cheatsUsedSummary: document.getElementById('cheats-used-summary'),
        playerNameInput: document.getElementById('player-name'),
        leaderboardWelcome: document.getElementById('leaderboard-list-welcome'),
        leaderboardGameOver: document.getElementById('leaderboard-list-gameover'),
        quitModal: document.getElementById('quit-modal'),
        highscoreModal: document.getElementById('highscore-modal'),
        welcomeDuration: document.getElementById('welcome-duration'),
    };
    
    // --- 2. Game State Variables ---
    let score = 0;
    let timeLeft = 0;
    let timerInterval = null;
    let correctStreak = 0;
    let seenQuestionIds = [];
    let currentCorrectAnswer = '';
    let penaltyPoints = 0;
    let cheatsUsed = 0;
    let gameHistory = {};
    let isInputPaused = false;
    let leaderboardData = [];
    let sounds = {};
    let isMuted = true;
    let currentScreen = 'welcome';
    let audioUnlocked = false;

    // --- 3. Sound Functions ---
    function unlockAudio() {
        if (audioUnlocked) return;
        console.log("Unlocking audio context for all sounds...");
        Object.values(sounds).forEach(sound => {
            sound.play().then(() => {
                sound.pause();
                sound.currentTime = 0;
            }).catch(() => {});
        });
        audioUnlocked = true;
    }

    function playSound(sound) { if (!isMuted && sound) { sound.currentTime = 0; sound.play().catch(error => console.error(`Sound playback failed: ${error.message}`)); } }
    function stopSound(sound) { if (sound) { sound.pause(); sound.currentTime = 0; } }

    // --- 4. Event Listeners ---
    buttons.start.addEventListener('click', startGame);
    buttons.cheat.addEventListener('click', useCheat);
    displays.answerGrid.addEventListener('click', handleAnswerClick);
    buttons.saveScore.addEventListener('click', saveScore);
    buttons.skipScore.addEventListener('click', showGameOverScreen);
    buttons.quit.addEventListener('click', () => displays.quitModal.classList.remove('hidden'));
    buttons.quitConfirmNo.addEventListener('click', () => displays.quitModal.classList.add('hidden'));
    buttons.quitConfirmYes.addEventListener('click', quitGame);
    buttons.playAgain.addEventListener('click', startGame);
    buttons.playAgainExit.addEventListener('click', startGame);
    buttons.soundToggle.addEventListener('click', toggleMute);

    // --- 5. Core Game Logic ---
    function switchScreen(screenName) {
        currentScreen = screenName;
        Object.values(screens).forEach(screen => screen.classList.remove('active'));
        screens[screenName].classList.add('active');
        if (['welcome', 'gameOver', 'exit'].includes(screenName)) { playSound(sounds.lobby); } 
        else { stopSound(sounds.lobby); }
    }

    function toggleMute() {
        isMuted = !isMuted;
        buttons.soundToggle.textContent = isMuted ? '🔇' : '🔊';
        if (!isMuted) {
            unlockAudio();
            if (['welcome', 'gameOver', 'exit'].includes(currentScreen)) playSound(sounds.lobby);
        } 
        else { Object.values(sounds).forEach(stopSound); }
    }

    function startGame() {
        unlockAudio();
        if (timerInterval) clearInterval(timerInterval);
        score = 0;
        timeLeft = config.gameDuration;
        correctStreak = 0;
        seenQuestionIds = [];
        penaltyPoints = 0;
        cheatsUsed = 0;
        gameHistory = {};
        isInputPaused = false;
        displays.currentScore.textContent = score;
        displays.timeLeft.textContent = timeLeft;
        timerInterval = setInterval(updateTimer, 1000);
        playSound(sounds.start);
        fetchNewQuestion();
        switchScreen('game');
    }

    function updateTimer() {
        timeLeft--;
        displays.timeLeft.textContent = timeLeft;

        if (timeLeft === 5) {
            console.log("Attempting to play end sound...");
            playSound(sounds.end);
        }

        if (timeLeft <= 0) {
            endGame();
        }
    }

    async function fetchAndDisplayLeaderboard() {
        try {
            const response = await fetch(`/get_leaderboard?limit=${config.leaderboardEntries}`);
            leaderboardData = await response.json();
            const renderTarget = (listElement) => {
                listElement.innerHTML = '';
                if (leaderboardData.length === 0) { listElement.innerHTML = '<p>No scores yet. Be the first!</p>'; return; }
                leaderboardData.forEach(entry => {
                    const item = document.createElement('div');
                    item.className = 'leaderboard-item';
                    item.innerHTML = `<span class="leaderboard-name">${entry.player_name}</span><span class="leaderboard-score">${entry.score}</span>`;
                    listElement.appendChild(item);
                });
            };
            renderTarget(displays.leaderboardWelcome);
            renderTarget(displays.leaderboardGameOver);
        } catch (error) {
            console.error("Failed to fetch leaderboard:", error);
            displays.leaderboardWelcome.innerHTML = '<p>Could not load scores.</p>';
            displays.leaderboardGameOver.innerHTML = '<p>Could not load scores.</p>';
        }
    }

    async function fetchNewQuestion() {
        isInputPaused = true;
        try {
            const response = await fetch(`/get_question?seen_ids=${seenQuestionIds.join(',')}`);
            if (!response.ok) throw new Error(`Server error: ${response.statusText}`);
            const data = await response.json();
            if (data.error) { if (data.error === "No more questions available") endGame(); throw new Error(data.error); }
            seenQuestionIds.push(data.id);
            currentCorrectAnswer = data.correct_answer;
            gameHistory[data.id] = { correctAnswer: data.correct_answer, tries: 0, answeredCorrectly: false, };
            renderQuestion(data);
        } catch (error) {
            console.error("Failed to fetch question:", error);
            displays.answerGrid.innerHTML = `<p style="color: var(--accent-color);">Error: ${error.message}</p>`;
        } finally {
            setTimeout(() => { isInputPaused = false; }, 200);
        }
    }

    function renderQuestion(data) {
        displays.questionImage.src = data.visual;
        displays.answerGrid.innerHTML = '';
        data.answers.forEach(answer => {
            const button = document.createElement('button');
            button.className = 'answer-btn';
            button.textContent = answer;
            displays.answerGrid.appendChild(button);
        });
    }

    function handleAnswerClick(event) {
        if (isInputPaused || !event.target.classList.contains('answer-btn')) return;
        isInputPaused = true;
        const clickedButton = event.target;
        const isCorrect = clickedButton.textContent === currentCorrectAnswer;
        const questionId = seenQuestionIds[seenQuestionIds.length - 1];
        if (questionId && gameHistory[questionId]) {
            gameHistory[questionId].tries++;
            if (isCorrect) gameHistory[questionId].answeredCorrectly = true;
        }
        if (isCorrect) handleCorrectAnswer(clickedButton);
        else handleIncorrectAnswer(clickedButton);
    }

    function handleCorrectAnswer(button) {
        playSound(sounds.correct);
        button.classList.add('correct');
        score += config.pointsPerAnswer;
        displays.currentScore.textContent = score;
        correctStreak++;
        showFeedback("Correct!", "correct");
        if (correctStreak === config.streakRequirement) {
            timeLeft += config.streakBonus;
            displays.timeLeft.textContent = timeLeft;
            correctStreak = 0;
            playSound(sounds.bonus);
            showFeedback(`+${config.streakBonus}s Bonus!`, "bonus");
        }
        setTimeout(fetchNewQuestion, 800);
    }

    function handleIncorrectAnswer(button) {
        playSound(sounds.wrong);
        button.classList.add('incorrect');
        button.disabled = true;
        penaltyPoints += config.penaltyPerWrongPoints;
        correctStreak = 0;
        const allButtons = displays.answerGrid.querySelectorAll('.answer-btn');
        allButtons.forEach(btn => btn.disabled = true);
        setTimeout(() => {
            allButtons.forEach(btn => {
                if (!btn.classList.contains('incorrect') && !btn.classList.contains('cheat-hidden')) {
                    btn.disabled = false;
                }
            });
            isInputPaused = false;
        }, config.penaltyPerWrongSeconds * 1000);
    }

    function useCheat() {
        if (isInputPaused || timeLeft <= config.cheatCost) return;
        playSound(sounds.cheat);
        timeLeft -= config.cheatCost;
        displays.timeLeft.textContent = timeLeft;
        cheatsUsed++;
        const wrongButtons = Array.from(displays.answerGrid.querySelectorAll('.answer-btn')).filter(btn => btn.textContent !== currentCorrectAnswer);
        wrongButtons.sort(() => 0.5 - Math.random());
        for (let i = 0; i < config.cheatAnswersRemoved && i < wrongButtons.length; i++) {
            wrongButtons[i].disabled = true;
            wrongButtons[i].classList.add('cheat-hidden');
        }
    }

    function showFeedback(message, type) {
        displays.feedbackMessage.textContent = message;
        displays.feedbackMessage.style.color = type === 'bonus' ? 'var(--gold-color)' : 'var(--correct-color)';
        displays.feedbackMessage.style.opacity = 1;
        setTimeout(() => { displays.feedbackMessage.style.opacity = 0; }, 1500);
    }

    function endGame() {
        if (timerInterval) clearInterval(timerInterval);
        timerInterval = null;
        stopSound(sounds.end);
        const finalScoreValue = Math.max(0, score - penaltyPoints);
        displays.finalScore.textContent = finalScoreValue;
        displays.cheatsUsedSummary.textContent = cheatsUsed;
        renderGameSummary();
        checkLeaderboardEligibility(finalScoreValue);
    }

    function checkLeaderboardEligibility(currentScore) {
        const lowestScore = leaderboardData.length < config.leaderboardEntries ? 0 : leaderboardData[leaderboardData.length - 1].score;
        if (currentScore > 0 && currentScore >= lowestScore) {
            displays.playerNameInput.value = '';
            buttons.saveScore.disabled = false;
            buttons.saveScore.textContent = 'Save to Hall of Fame';
            displays.highscoreModal.classList.remove('hidden');
        } else {
            showGameOverScreen();
        }
    }

    function showGameOverScreen() {
        displays.highscoreModal.classList.add('hidden');
        switchScreen('gameOver');
    }

    function quitGame() {
        if (timerInterval) clearInterval(timerInterval);
        stopSound(sounds.end);
        displays.quitModal.classList.add('hidden');
        displays.exitScore.textContent = score;
        switchScreen('exit');
    }

    function renderGameSummary() {
        displays.summaryDetails.innerHTML = '';
        const attemptedQuestionIds = seenQuestionIds.filter(id => gameHistory[id] && gameHistory[id].tries > 0);

        if (attemptedQuestionIds.length === 0) {
            displays.summaryDetails.innerHTML = '<p>You didn\'t answer any questions.</p>';
            return;
        }

        attemptedQuestionIds.forEach(id => {
            const item = gameHistory[id];
            const resultIcon = item.answeredCorrectly ? '✅' : '❌';
            const triesText = item.tries === 1 ? '1 try' : `${item.tries} tries`;
            const summaryItem = document.createElement('div');
            summaryItem.innerHTML = `${resultIcon} <strong>${item.correctAnswer}</strong> (${triesText})`;
            displays.summaryDetails.appendChild(summaryItem);
        });
    }

    async function saveScore() {
        const playerName = displays.playerNameInput.value.trim();
        const finalScore = parseInt(displays.finalScore.textContent, 10);
        if (!playerName) { alert("Please enter your name!"); return; }
        buttons.saveScore.disabled = true;
        buttons.saveScore.textContent = 'Saving...';
        try {
            const response = await fetch('/submit_score', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ playerName, score: finalScore }), });
            if (!response.ok) throw new Error('Failed to save score.');
            const result = await response.json();
            if (result.success) {
                await fetchAndDisplayLeaderboard();
                showGameOverScreen();
            } else {
                throw new Error(result.error || 'Unknown error');
            }
        } catch (error) {
            console.error('Error saving score:', error);
            buttons.saveScore.textContent = 'Error!';
        }
    }

    // --- Initial Load Function ---
    async function initializeApp() {
        try {
            const response = await fetch('/static/config.json');
            if (!response.ok) throw new Error('config.json not found');
            config = await response.json();

            const configSpans = {
                points: document.getElementById('config-points'),
                streakReq: document.getElementById('config-streak-req'),
                streakBonus: document.getElementById('config-streak-bonus'),
                penaltyTime: document.getElementById('config-penalty-time'),
                penaltyPoints: document.getElementById('config-penalty-points'),
                cheatRemoved: document.getElementById('config-cheat-removed'),
                cheatCost: document.getElementById('config-cheat-cost'),
                cheatCostBtn: document.getElementById('config-cheat-cost-btn'),
            };

            Object.keys(config.sounds).forEach(key => {
                sounds[key] = new Audio(config.sounds[key]);
            });
            if (sounds.lobby) sounds.lobby.loop = true;

            displays.timeLeft.textContent = config.gameDuration;
            displays.welcomeDuration.textContent = config.gameDuration;
            configSpans.points.textContent = config.pointsPerAnswer;
            configSpans.streakReq.textContent = config.streakRequirement;
            configSpans.streakBonus.textContent = config.streakBonus;
            configSpans.penaltyTime.textContent = config.penaltyPerWrongSeconds;
            configSpans.penaltyPoints.textContent = config.penaltyPerWrongPoints;
            configSpans.cheatRemoved.textContent = config.cheatAnswersRemoved;
            configSpans.cheatCost.textContent = config.cheatCost;
            configSpans.cheatCostBtn.textContent = config.cheatCost;


            await fetchAndDisplayLeaderboard();
            switchScreen('welcome');

        } catch (error) {
            console.error("Fatal Error: Could not load game configuration.", error);
            document.body.innerHTML = `<h1 style="color: red; text-align: center;">Error: Could not load game config.json. Please check the file and refresh.</h1>`;
        }
    }

    initializeApp();
});
